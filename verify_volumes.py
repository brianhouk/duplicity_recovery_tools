#!/usr/bin/env python3
"""
Simple standalone tool to verify duplicity backup volumes.

Compares SHA1 hashes from manifest files against either:
- Pre-computed checksums in sha1sums.txt file
- Freshly computed checksums from volume files

Lists all files contained in corrupted volumes.
"""

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def decrypt_manifest(manifest_path: Path, passphrase: Optional[str] = None) -> str:
    """Decrypt a GPG-encrypted manifest file."""
    cmd = ['gpg', '--decrypt', '--batch', '--quiet']
    if passphrase:
        cmd.extend(['--passphrase-fd', '0'])
        result = subprocess.run(
            cmd + [str(manifest_path)],
            input=passphrase.encode(),
            capture_output=True,
            check=True
        )
    else:
        result = subprocess.run(
            cmd + [str(manifest_path)],
            capture_output=True,
            check=True
        )
    return result.stdout.decode('utf-8')


def parse_manifest(content: str) -> Tuple[Dict[int, Dict], List[Dict]]:
    """
    Parse manifest content to extract volume info and file list.

    Returns:
        Tuple of (volumes dict, files list)
        volumes: {vol_num: {'hash': str, 'start_path': str, 'end_path': str}}
        files: [{'path': str, 'status': str}]
    """
    volumes = {}
    files = []

    lines = content.split('\n')
    in_filelist = False
    current_volume = None

    for line in lines:
        # Check for filelist section
        if line.strip() == 'Filelist':
            in_filelist = True
            continue

        if in_filelist:
            # Parse file entries: "status path"
            match = re.match(r'^(\S+)\s+(.+)$', line.strip())
            if match:
                status, path = match.groups()
                files.append({'path': path, 'status': status})
        else:
            # Parse volume entries
            if line.startswith('Volume '):
                # Extract volume number
                match = re.search(r'Volume (\d+):', line)
                if match:
                    vol_num = int(match.group(1))
                    current_volume = vol_num
                    volumes[vol_num] = {}

            elif current_volume is not None:
                # Parse volume properties
                if 'Hash SHA1' in line or 'Hash MD5' in line:
                    hash_val = line.split()[-1]
                    volumes[current_volume]['hash'] = hash_val

                elif 'StartingPath' in line:
                    # Extract path (everything after the first space)
                    parts = line.split(None, 1)
                    if len(parts) > 1:
                        volumes[current_volume]['start_path'] = parts[1].strip()

                elif 'EndingPath' in line:
                    parts = line.split(None, 1)
                    if len(parts) > 1:
                        volumes[current_volume]['end_path'] = parts[1].strip()

    return volumes, files


def parse_checksum_file(checksum_path: Path) -> Dict[str, str]:
    """
    Parse sha1sum-format checksum file.

    Format: "hash  filename" or "hash *filename"
    Returns: {filename: hash}
    """
    checksums = {}
    with open(checksum_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # sha1sum format: "hash  filename" or "hash *filename"
            parts = re.split(r'\s+', line, maxsplit=1)
            if len(parts) == 2:
                hash_val, filename = parts
                # Remove leading * or space from filename
                filename = filename.lstrip('* ')
                # Store just the basename
                checksums[Path(filename).name] = hash_val.lower()

    return checksums


def compute_sha1(file_path: Path) -> str:
    """Compute SHA1 hash of a file."""
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha1.update(chunk)
    return sha1.hexdigest()


def find_volume_file(backup_dir: Path, vol_num: int) -> Optional[Path]:
    """Find a volume file in the backup directory."""
    # Look for volume files matching the pattern
    patterns = [
        f'*vol{vol_num}.difftar.gpg',
        f'*.vol{vol_num}.difftar.gpg'
    ]

    for pattern in patterns:
        matches = list(backup_dir.glob(pattern))
        if matches:
            return matches[0]

    return None


def filter_files_by_path_range(files: List[Dict], start_path: str, end_path: str) -> List[Dict]:
    """Filter files that fall within a path range."""
    affected = []

    for file_entry in files:
        path = file_entry['path']
        # Simple lexicographic comparison (duplicity uses tuple comparison, but this works for most cases)
        if start_path <= path <= end_path:
            affected.append(file_entry)

    return affected


def main():
    parser = argparse.ArgumentParser(
        description='Verify duplicity backup volumes against manifest hashes'
    )
    parser.add_argument(
        'backup_dir',
        type=Path,
        help='Directory containing duplicity backup files'
    )
    parser.add_argument(
        '--manifest',
        type=Path,
        help='Specific manifest file (default: find *.manifest.gpg in backup_dir)'
    )
    parser.add_argument(
        '--checksum-file',
        type=Path,
        help='Use pre-computed checksums from sha1sums.txt file'
    )
    parser.add_argument(
        '--passphrase',
        help='GPG passphrase (or will use GPG agent)'
    )
    parser.add_argument(
        '--show-all',
        action='store_true',
        help='Show all volumes including verified ones'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    # Find manifest
    if args.manifest:
        manifest_path = args.manifest
    else:
        manifests = list(args.backup_dir.glob('*.manifest.gpg'))
        if not manifests:
            print(f"Error: No manifest files found in {args.backup_dir}", file=sys.stderr)
            return 1
        manifest_path = manifests[0]
        if len(manifests) > 1:
            print(f"Warning: Multiple manifests found, using {manifest_path.name}")

    print(f"Using manifest: {manifest_path.name}")
    print()

    # Load pre-computed checksums if provided
    precomputed_checksums = None
    if args.checksum_file:
        if args.verbose:
            print(f"Loading checksums from {args.checksum_file}...")
        precomputed_checksums = parse_checksum_file(args.checksum_file)
        print(f"Loaded {len(precomputed_checksums)} pre-computed checksums")
        print()

    # Decrypt and parse manifest
    if args.verbose:
        print("Decrypting manifest...")
    manifest_content = decrypt_manifest(manifest_path, args.passphrase)

    if args.verbose:
        print("Parsing manifest...")
    volumes, files = parse_manifest(manifest_content)

    print(f"Found {len(volumes)} volumes in manifest")
    print(f"Found {len(files)} files in filelist")
    print()

    # Verify each volume
    verified = 0
    corrupted = 0
    missing = 0
    corrupted_volumes = []

    for vol_num in sorted(volumes.keys()):
        vol_info = volumes[vol_num]

        if 'hash' not in vol_info:
            if args.verbose:
                print(f"Volume {vol_num}: Skipping (no hash in manifest)")
            continue

        expected_hash = vol_info['hash'].lower()

        # Find volume file
        vol_file = find_volume_file(args.backup_dir, vol_num)
        if not vol_file:
            missing += 1
            print(f"Volume {vol_num}: MISSING")
            continue

        # Get or compute checksum
        if precomputed_checksums:
            calculated_hash = precomputed_checksums.get(vol_file.name)
            if calculated_hash is None:
                print(f"Volume {vol_num}: ERROR - not in checksum file")
                continue
        else:
            if args.verbose:
                print(f"Volume {vol_num}: Computing SHA1...", end='', flush=True)
            calculated_hash = compute_sha1(vol_file)
            if args.verbose:
                print(" done")

        # Compare hashes
        if calculated_hash == expected_hash:
            verified += 1
            if args.show_all:
                print(f"Volume {vol_num}: OK")
        else:
            corrupted += 1
            corrupted_volumes.append(vol_num)
            print(f"Volume {vol_num}: CORRUPTED")
            print(f"  Expected:   {expected_hash}")
            print(f"  Calculated: {calculated_hash}")

            # Show affected files
            if 'start_path' in vol_info and 'end_path' in vol_info:
                start_path = vol_info['start_path']
                end_path = vol_info['end_path']
                print(f"  Path range: {start_path} to {end_path}")

                affected = filter_files_by_path_range(files, start_path, end_path)
                print(f"  Affected files ({len(affected)}):")
                for file_entry in affected:
                    print(f"    [{file_entry['status']:>7}] {file_entry['path']}")
            print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total volumes:  {len(volumes)}")
    print(f"Verified (OK):  {verified}")
    print(f"Corrupted:      {corrupted}")
    print(f"Missing:        {missing}")

    if corrupted_volumes:
        print()
        print(f"Corrupted volume numbers: {','.join(str(v) for v in corrupted_volumes)}")

    return 1 if (corrupted > 0 or missing > 0) else 0


if __name__ == '__main__':
    sys.exit(main())
