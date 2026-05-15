import json
import os
import tempfile
import unittest

import BrandINVEmailer as emailer


class FakeRequest:
    def __init__(self, callback):
        self.callback = callback

    def execute(self):
        return self.callback()


class FakePermissions:
    def __init__(self):
        self.current = {}
        self.created = []
        self.deleted = []

    def list(self, fileId, fields=None):
        return FakeRequest(lambda: {"permissions": list(self.current.get(fileId, []))})

    def create(self, fileId, body):
        def callback():
            permission_id = f"perm-{len(self.created) + 1}"
            self.created.append({"fileId": fileId, "body": dict(body), "id": permission_id})
            self.current.setdefault(fileId, []).append({"id": permission_id, **body})
            return {"id": permission_id}

        return FakeRequest(callback)

    def delete(self, fileId, permissionId):
        def callback():
            self.deleted.append({"fileId": fileId, "permissionId": permissionId})
            self.current[fileId] = [
                permission
                for permission in self.current.get(fileId, [])
                if permission.get("id") != permissionId
            ]
            return {}

        return FakeRequest(callback)


class FakeFiles:
    def __init__(self):
        self.created = []
        self.next_id = 1

    def list(self, q, spaces="drive", fields=None):
        return FakeRequest(lambda: {"files": []})

    def create(self, body, fields="id"):
        def callback():
            folder_id = f"folder-{self.next_id}"
            self.next_id += 1
            self.created.append({"id": folder_id, "body": dict(body)})
            return {"id": folder_id}

        return FakeRequest(callback)


class FakeDriveService:
    def __init__(self):
        self.permissions_api = FakePermissions()
        self.files_api = FakeFiles()

    def permissions(self):
        return self.permissions_api

    def files(self):
        return self.files_api


class BrandInvOtherFolderTests(unittest.TestCase):
    def test_domain_other_folder_removes_public_access(self):
        service = FakeDriveService()
        service.permissions_api.current["other-folder"] = [
            {"id": "anyoneWithLink", "type": "anyone", "role": "reader"}
        ]

        emailer.make_folder_domain_viewable(service, "other-folder", "buzzcannabis.com")

        self.assertEqual(
            service.permissions_api.deleted,
            [{"fileId": "other-folder", "permissionId": "anyoneWithLink"}],
        )
        self.assertEqual(service.permissions_api.created[0]["body"]["type"], "domain")
        self.assertEqual(service.permissions_api.created[0]["body"]["domain"], "buzzcannabis.com")
        self.assertFalse(service.permissions_api.created[0]["body"]["allowFileDiscovery"])
        self.assertNotIn(
            "anyone",
            [permission.get("type") for permission in service.permissions_api.current["other-folder"]],
        )

    def test_public_brand_folder_gets_anyone_link_permission(self):
        service = FakeDriveService()

        folder_id = emailer.find_or_create_folder(
            service,
            "710 Labs",
            parent_id="other-folder",
            make_public=True,
        )

        self.assertEqual(folder_id, "folder-1")
        self.assertEqual(service.files_api.created[0]["body"]["parents"], ["other-folder"])
        self.assertEqual(service.permissions_api.created[0]["fileId"], "folder-1")
        self.assertEqual(service.permissions_api.created[0]["body"], {"type": "anyone", "role": "reader"})

    def test_other_folder_settings_default_enabled_and_can_disable(self):
        self.assertEqual(
            emailer.load_other_folder_settings({}),
            {
                "enabled": True,
                "parent_folder_name": "INVENTORY_OTHER",
                "folder_name": "OTHER",
                "domain": "buzzcannabis.com",
            },
        )
        self.assertFalse(emailer.load_other_folder_settings({"other_folder": False})["enabled"])

    def test_other_only_cli_flag_is_available(self):
        args = emailer.build_arg_parser().parse_args(["--other-only", "--no-refresh"])

        self.assertTrue(args.other_only)
        self.assertTrue(args.no_refresh)

    def test_other_folder_email_intro_explains_first_link_and_sharing(self):
        intro = emailer.build_other_folder_email_intro(
            "https://drive.google.com/drive/folders/other",
            other_folder_name="OTHER",
            other_domain="buzzcannabis.com",
        )
        email_fragment = intro + "<p>Link: <a href='https://drive.google.com/drive/folders/brand'>brand</a></p>"

        self.assertLess(
            email_fragment.index("https://drive.google.com/drive/folders/other"),
            email_fragment.index("https://drive.google.com/drive/folders/brand"),
        )
        self.assertIn("First link", intro)
        self.assertIn("not listed below", intro)
        self.assertIn("buzzcannabis.com", intro)
        self.assertIn("anyone with that direct brand-folder", intro)

    def test_inventory_email_subject_stays_simple(self):
        self.assertEqual(
            emailer.build_inventory_email_subject("Friday"),
            "Brand Inventory Reports for Friday",
        )

    def test_safe_report_filename_part_handles_drive_ok_but_local_bad_characters(self):
        self.assertEqual(emailer.safe_report_filename_part(" YoCan / Special. "), "YoCan - Special")
        self.assertEqual(emailer.safe_report_filename_part(""), "Unknown")

    def test_groups_generated_workbooks_by_parsed_brand_name(self):
        grouped = emailer.group_generated_files_by_brand(
            [
                "/tmp/MV_710 Labs_05-15-2026.xlsx",
                "/tmp/SV_710 Labs_05-15-2026.xlsx",
                "/tmp/LM_Raw Garden_05-15-2026.xlsx",
                "/tmp/MV_YoCan - Special_05-15-2026.xlsx",
                "/tmp/not-a-brand-file.txt",
            ]
        )

        self.assertEqual(
            grouped,
            {
                "710 Labs": [
                    "/tmp/MV_710 Labs_05-15-2026.xlsx",
                    "/tmp/SV_710 Labs_05-15-2026.xlsx",
                ],
                "Raw Garden": ["/tmp/LM_Raw Garden_05-15-2026.xlsx"],
                "YoCan - Special": ["/tmp/MV_YoCan - Special_05-15-2026.xlsx"],
            },
        )

    def test_manifest_includes_other_folder_and_child_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            old_links_dir = emailer.INVENTORY_LINKS_DIR
            emailer.INVENTORY_LINKS_DIR = tmpdir
            try:
                emailer.write_inventory_link_manifest(
                    date_str="2026-05-15",
                    today_name="Friday",
                    brand_folder_links={"Hashish": "https://drive.google.com/drive/folders/hashish"},
                    brand_to_emails={"Hashish": ["donna@buzzcannabis.com"]},
                    other_folder_link="https://drive.google.com/drive/folders/other",
                    other_brand_folder_links={
                        "710 Labs": "https://drive.google.com/drive/folders/710",
                        "Raw Garden": "https://drive.google.com/drive/folders/raw",
                    },
                    other_parent_folder_name="INVENTORY_OTHER",
                    other_folder_name="OTHER",
                    other_domain="buzzcannabis.com",
                )

                with open(os.path.join(tmpdir, "latest.json"), "r", encoding="utf-8") as handle:
                    manifest = json.load(handle)
                with open(os.path.join(tmpdir, "latest_other_links.txt"), "r", encoding="utf-8") as handle:
                    other_links_text = handle.read()
            finally:
                emailer.INVENTORY_LINKS_DIR = old_links_dir

        self.assertEqual(manifest["other_folder"]["folder_name"], "OTHER")
        self.assertEqual(manifest["other_folder"]["parent_folder_name"], "INVENTORY_OTHER")
        self.assertEqual(manifest["other_folder"]["domain"], "buzzcannabis.com")
        self.assertEqual(
            manifest["other_folder"]["brand_folders"]["710 Labs"]["link"],
            "https://drive.google.com/drive/folders/710",
        )
        self.assertIn("OTHER (buzzcannabis.com users only): https://drive.google.com/drive/folders/other", other_links_text)
        self.assertIn("710 Labs: https://drive.google.com/drive/folders/710", other_links_text)


if __name__ == "__main__":
    unittest.main()
