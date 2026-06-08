import os
import sys
import tempfile
import types
import unittest

try:
    import BrandInventoryGUIemailer as gui
except ModuleNotFoundError as exc:
    missing_name = exc.name or ""
    if not (
        missing_name == "google"
        or missing_name.startswith("google.")
        or missing_name.startswith("google_")
        or missing_name == "googleapiclient"
        or missing_name.startswith("googleapiclient.")
    ):
        raise

    google = types.ModuleType("google")
    google_auth = types.ModuleType("google.auth")
    google_auth_transport = types.ModuleType("google.auth.transport")
    google_auth_transport_requests = types.ModuleType("google.auth.transport.requests")
    google_auth_transport_requests.Request = object
    google_auth.transport = google_auth_transport
    google_auth_transport.requests = google_auth_transport_requests
    google.auth = google_auth

    google_auth_oauthlib = types.ModuleType("google_auth_oauthlib")
    google_auth_oauthlib_flow = types.ModuleType("google_auth_oauthlib.flow")
    google_auth_oauthlib_flow.InstalledAppFlow = object
    google_auth_oauthlib.flow = google_auth_oauthlib_flow

    google_oauth2 = types.ModuleType("google.oauth2")
    google_oauth2_credentials = types.ModuleType("google.oauth2.credentials")
    google_oauth2_credentials.Credentials = object
    google.oauth2 = google_oauth2
    google_oauth2.credentials = google_oauth2_credentials

    googleapiclient = types.ModuleType("googleapiclient")
    googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
    googleapiclient_discovery.build = lambda *args, **kwargs: None
    googleapiclient_http = types.ModuleType("googleapiclient.http")
    googleapiclient_http.MediaFileUpload = lambda *args, **kwargs: object()
    googleapiclient_errors = types.ModuleType("googleapiclient.errors")
    googleapiclient_errors.HttpError = Exception
    googleapiclient.discovery = googleapiclient_discovery
    googleapiclient.http = googleapiclient_http
    googleapiclient.errors = googleapiclient_errors

    sys.modules.update(
        {
            "google": google,
            "google.auth": google_auth,
            "google.auth.transport": google_auth_transport,
            "google.auth.transport.requests": google_auth_transport_requests,
            "google_auth_oauthlib": google_auth_oauthlib,
            "google_auth_oauthlib.flow": google_auth_oauthlib_flow,
            "google.oauth2": google_oauth2,
            "google.oauth2.credentials": google_oauth2_credentials,
            "googleapiclient": googleapiclient,
            "googleapiclient.discovery": googleapiclient_discovery,
            "googleapiclient.http": googleapiclient_http,
            "googleapiclient.errors": googleapiclient_errors,
        }
    )

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
        self.created_folders = []
        self.deleted = []
        self.folder_queries = []
        self.uploads = []

    def list(self, q, spaces="drive", fields=None, pageToken=None):
        def callback():
            if "mimeType='application/vnd.google-apps.folder'" in q:
                self.folder_queries.append(q)
                if "name='INVENTORY'" in q:
                    return {"files": [{"id": "inventory-folder", "name": "INVENTORY"}]}
                if "name='2026-06-08'" in q and "'inventory-folder' in parents" in q:
                    return {"files": []}
                if "name='hashish'" in q and "'date-folder' in parents" in q:
                    return {"files": []}
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
                folder_id = {
                    "2026-06-08": "date-folder",
                    "hashish": "hashish-folder",
                }.get(body.get("name"), "unexpected-new-folder")
                self.created_folders.append({"id": folder_id, "body": dict(body)})
                return {"id": folder_id}
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
    def test_upload_places_brand_folder_inside_date_folder_and_replaces_contents(self):
        service = FakeDriveService()
        old_auth = gui.drive_authenticate
        gui.drive_authenticate = lambda: service
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                report_path = os.path.join(tmpdir, "MV_hashish_06-05-2026.xlsx")
                with open(report_path, "wb") as handle:
                    handle.write(b"fake workbook")

                links = gui.upload_brand_reports_to_drive(
                    {"hashish": [report_path]},
                    date_folder_name="2026-06-08",
                )
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
        self.assertEqual(
            service.files_api.created_folders,
            [
                {
                    "id": "date-folder",
                    "body": {
                        "name": "2026-06-08",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["inventory-folder"],
                    },
                },
                {
                    "id": "hashish-folder",
                    "body": {
                        "name": "hashish",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["date-folder"],
                    },
                },
            ],
        )
        self.assertTrue(
            any("name='hashish'" in query and "'date-folder' in parents" in query for query in service.files_api.folder_queries),
            "Brand folders must be created under the date folder, not directly under INVENTORY.",
        )


class BrandInventoryGUIGroupTests(unittest.TestCase):
    def test_load_brand_group_options_uses_folder_names_and_synonyms(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "brand_config2.json")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    """
                    {
                      "brands": [
                        {
                          "brand_synonyms": ["Cannabiotix (CBX)", "Heirbloom"],
                          "folder_name": "CBX / Heirbloom"
                        }
                      ]
                    }
                    """
                )

            groups = gui.load_brand_group_options(path)

        self.assertEqual(groups, [{"label": "CBX / Heirbloom", "synonyms": ["Cannabiotix (CBX)", "Heirbloom"]}])

    def test_group_matching_handles_parenthetical_aliases_and_punctuation(self):
        group = {
            "label": "Mixed",
            "synonyms": ["Cannabiotix (CBX)", "Papa & Barkley (P&B)", "KIVA", "Humo"],
        }

        matched, missing = gui.match_brand_group_to_loaded_brands(
            group,
            ["cbx", "papa barkley", "kiva", "raw garden"],
        )

        self.assertEqual(matched, ["cbx", "kiva", "papa barkley"])
        self.assertEqual(missing, ["Humo"])


if __name__ == "__main__":
    unittest.main()
