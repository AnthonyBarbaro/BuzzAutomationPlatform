import os
import tempfile
import unittest

import BrandInventoryGUIemailer as gui


class FakeRequest:
    def __init__(self, callback):
        self.callback = callback

    def execute(self):
        return self.callback()


class FakePermissions:
    def __init__(self):
        self.current = {}
        self.created = []

    def list(self, fileId, fields=None):
        return FakeRequest(lambda: {"permissions": list(self.current.get(fileId, []))})

    def create(self, fileId, body):
        def callback():
            permission_id = f"perm-{len(self.created) + 1}"
            self.created.append({"fileId": fileId, "body": dict(body), "id": permission_id})
            self.current.setdefault(fileId, []).append({"id": permission_id, **body})
            return {"id": permission_id}

        return FakeRequest(callback)


class FakeFiles:
    def __init__(self):
        self.deleted = []
        self.folder_queries = []
        self.uploads = []

    def list(self, q, spaces="drive", fields=None, pageToken=None):
        def callback():
            if "mimeType='application/vnd.google-apps.folder'" in q:
                self.folder_queries.append(q)
                if "name='INVENTORY'" in q:
                    return {"files": [{"id": "inventory-folder", "name": "INVENTORY"}]}
                if "name='hashish'" in q and "'inventory-folder' in parents" in q:
                    return {"files": [{"id": "hashish-folder", "name": "hashish"}]}
                return {"files": []}

            if "'hashish-folder' in parents" in q:
                return {
                    "files": [
                        {"id": "old-file-1", "name": "old mv.xlsx"},
                        {"id": "old-file-2", "name": "old sv.xlsx"},
                    ]
                }
            return {"files": []}

        return FakeRequest(callback)

    def create(self, body, media_body=None, fields="id"):
        def callback():
            if body.get("mimeType") == "application/vnd.google-apps.folder":
                return {"id": "unexpected-new-folder"}
            self.uploads.append({"body": dict(body), "has_media": media_body is not None})
            return {"id": f"uploaded-{len(self.uploads)}"}

        return FakeRequest(callback)

    def delete(self, fileId):
        def callback():
            self.deleted.append(fileId)
            return {}

        return FakeRequest(callback)


class FakeDriveService:
    def __init__(self):
        self.files_api = FakeFiles()
        self.permissions_api = FakePermissions()

    def files(self):
        return self.files_api

    def permissions(self):
        return self.permissions_api


class BrandInventoryGUIDriveTests(unittest.TestCase):
    def test_upload_reuses_stable_brand_folder_and_replaces_contents(self):
        service = FakeDriveService()
        old_auth = gui.drive_authenticate
        gui.drive_authenticate = lambda: service
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                report_path = os.path.join(tmpdir, "MV_hashish_06-05-2026.xlsx")
                with open(report_path, "wb") as handle:
                    handle.write(b"fake workbook")

                links = gui.upload_brand_reports_to_drive({"hashish": [report_path]})
        finally:
            gui.drive_authenticate = old_auth

        self.assertEqual(
            links,
            {"hashish": "https://drive.google.com/drive/folders/hashish-folder"},
        )
        self.assertEqual(service.files_api.deleted, ["old-file-1", "old-file-2"])
        self.assertEqual(len(service.files_api.uploads), 1)
        self.assertEqual(service.files_api.uploads[0]["body"]["parents"], ["hashish-folder"])
        self.assertTrue(service.files_api.uploads[0]["has_media"])
        self.assertEqual(service.permissions_api.created[0]["fileId"], "hashish-folder")
        self.assertFalse(
            any("name='2026-" in query for query in service.files_api.folder_queries),
            "GUI uploads should use stable brand folders instead of date folders.",
        )


if __name__ == "__main__":
    unittest.main()
