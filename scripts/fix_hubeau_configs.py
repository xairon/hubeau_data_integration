#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fix hubeau_configs.py - Remove pagination parameters safely

This script uses line-by-line processing to avoid formatting corruption.
"""

from pathlib import Path
import sys
import io

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

BASE_DIR = Path(__file__).parent.parent
HUBEAU_CONFIGS = BASE_DIR / "src/hubeau_pipeline/assets/bronze/hubeau_configs.py"

def fix_hubeau_configs():
    """Fix hubeau_configs.py by removing pagination parameters line-by-line"""

    print("="*70)
    print("🔧 FIXING hubeau_configs.py")
    print("="*70)
    print(f"\n📝 Reading {HUBEAU_CONFIGS}...")

    content = HUBEAU_CONFIGS.read_text(encoding='utf-8')
    lines = content.split('\n')

    print(f"📊 Original: {len(lines)} lines")

    output_lines = []
    skip_until_closing_brace = False
    removed_count = 0

    for i, line in enumerate(lines):
        # Check if we're inside a spatial_params dict
        if skip_until_closing_brace:
            if '}' in line:
                skip_until_closing_brace = False
            removed_count += 1
            print(f"  🗑️  Line {i+1}: Skipping spatial_params content")
            continue

        # Check for parameters to remove
        if 'page_size=' in line and 'HubeauEndpointConfig' not in line:
            print(f"  🗑️  Line {i+1}: Removing page_size")
            removed_count += 1
            continue
        elif 'max_pages=' in line:
            print(f"  🗑️  Line {i+1}: Removing max_pages")
            removed_count += 1
            continue
        elif 'depth_limit=' in line:
            print(f"  🗑️  Line {i+1}: Removing depth_limit")
            removed_count += 1
            continue
        elif 'requires_spatial_filter=' in line:
            print(f"  🗑️  Line {i+1}: Removing requires_spatial_filter")
            removed_count += 1
            continue
        elif 'spatial_params=' in line:
            print(f"  🗑️  Line {i+1}: Removing spatial_params")
            removed_count += 1
            if '{' in line and '}' not in line:
                # Multi-line dict, skip until closing brace
                skip_until_closing_brace = True
            continue

        # Keep this line
        output_lines.append(line)

    # Join lines back
    new_content = '\n'.join(output_lines)

    print(f"\n💾 Writing fixed content...")
    HUBEAU_CONFIGS.write_text(new_content, encoding='utf-8')

    print(f"\n✅ Fixed: {len(lines)} → {len(output_lines)} lines")
    print(f"📉 Removed: {removed_count} lines")

    # Verify syntax
    print(f"\n🔍 Verifying Python syntax...")
    import py_compile
    try:
        py_compile.compile(str(HUBEAU_CONFIGS), doraise=True)
        print(f"✅ Syntax OK!")
        return True
    except py_compile.PyCompileError as e:
        print(f"❌ Syntax error: {e}")
        return False

if __name__ == "__main__":
    print("\n🚀 Starting hubeau_configs.py fix script...\n")
    success = fix_hubeau_configs()

    if success:
        print("\n" + "="*70)
        print("✅ SCRIPT COMPLETED SUCCESSFULLY!")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("❌ SCRIPT FAILED - SEE ERRORS ABOVE")
        print("="*70)

    sys.exit(0 if success else 1)
