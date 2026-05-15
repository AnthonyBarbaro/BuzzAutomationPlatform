#!/usr/bin/env python3
"""
Create a small live Google Drive smoke test for the BrandINVEmailer OTHER flow.

This does not refresh Dutchie, generate inventory reports, or send email. It
only creates:

  INVENTORY_OTHER_PERMISSION_TESTS/<timestamp>/OTHER/<TEST BRAND PUBLIC LINK>

Expected manual check:
- OTHER link: only buzzcannabis.com users with the link should be able to browse.
- Child brand link: anyone with the link should be able to open it.
"""

import argparse
import datetime
import os
import tempfile

from BrandINVEmailer import (
    DEFAULT_OTHER_FOLDER_DOMAIN,
    DEFAULT_OTHER_FOLDER_NAME,
    drive_authenticate,
    drive_folder_link,
    find_or_create_folder,
    upload_file_to_drive,
)


TEST_PARENT_FOLDER_NAME = "INVENTORY_OTHER_PERMISSION_TESTS"
TEST_BRAND_FOLDER_NAME = "TEST BRAND PUBLIC LINK"


def build_parser():
    parser = argparse.ArgumentParser(description="Live Drive smoke test for OTHER-folder permissions.")
    parser.add_argument("--domain", default=DEFAULT_OTHER_FOLDER_DOMAIN, help="Workspace domain for the OTHER folder.")
    parser.add_argument("--other-folder", default=DEFAULT_OTHER_FOLDER_NAME, help="Name for the restricted OTHER folder.")
    parser.add_argument("--parent-folder", default=TEST_PARENT_FOLDER_NAME, help="Top-level test parent folder name.")
    parser.add_argument("--brand-folder", default=TEST_BRAND_FOLDER_NAME, help="Public child brand folder name.")
    return parser


def main():
    args = build_parser().parse_args()
    service = drive_authenticate()
    timestamp = datetime.datetime.now().strftime("OTHER_PERMISSION_TEST_%Y-%m-%d_%H%M%S")

    parent_id = find_or_create_folder(service, args.parent_folder, remove_public=True)
    if not parent_id:
        raise RuntimeError(f"Could not create/find {args.parent_folder}")

    run_id = find_or_create_folder(service, timestamp, parent_id=parent_id, remove_public=True)
    if not run_id:
        raise RuntimeError(f"Could not create/find {timestamp}")

    other_id = find_or_create_folder(
        service,
        args.other_folder,
        parent_id=run_id,
        domain_view=args.domain,
    )
    if not other_id:
        raise RuntimeError(f"Could not create/find {args.other_folder}")

    brand_id = find_or_create_folder(
        service,
        args.brand_folder,
        parent_id=other_id,
        make_public=True,
    )
    if not brand_id:
        raise RuntimeError(f"Could not create/find {args.brand_folder}")

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as handle:
        handle.write("BrandINVEmailer OTHER permission smoke test.\n")
        sample_path = handle.name

    try:
        upload_file_to_drive(service, sample_path, brand_id)
    finally:
        try:
            os.remove(sample_path)
        except OSError:
            pass

    print("\nCreated live Drive smoke test folders.")
    print(f"OTHER link ({args.domain} users only): {drive_folder_link(other_id)}")
    print(f"Public child brand link: {drive_folder_link(brand_id)}")
    print("\nManual check:")
    print("1. Open the OTHER link while signed into a buzzcannabis.com account.")
    print("2. Open the public child brand link in an incognito/private browser window.")


if __name__ == "__main__":
    main()
