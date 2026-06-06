def escape_drive_query_text(value):
    return str(value or "").replace("\\", "\\\\").replace("'", "\\'")


def list_drive_folder_children(service, folder_id):
    query = f"'{escape_drive_query_text(folder_id)}' in parents and trashed=false"
    children = []
    page_token = None

    while True:
        request_kwargs = {
            "q": query,
            "spaces": "drive",
            "fields": "nextPageToken, files(id, name, mimeType)",
        }
        if page_token:
            request_kwargs["pageToken"] = page_token

        response = service.files().list(**request_kwargs).execute()
        children.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return children


def clear_drive_folder_contents(service, folder_id, folder_label=None):
    label = folder_label or folder_id
    try:
        children = list_drive_folder_children(service, folder_id)
    except Exception as exc:
        print(f"[WARN] Could not list existing Drive folder contents for {label}: {exc}")
        return 0

    deleted_count = 0
    for child in children:
        child_id = child.get("id")
        child_name = child.get("name") or child_id
        if not child_id:
            continue
        try:
            service.files().delete(fileId=child_id).execute()
            deleted_count += 1
        except Exception as exc:
            print(f"[WARN] Could not remove old Drive item {child_name} from {label}: {exc}")

    if deleted_count:
        print(f"[INFO] Cleared {deleted_count} old Drive item(s) from {label}.")
    return deleted_count
