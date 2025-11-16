#!/usr/bin/env python3
"""
Validation script to check for invisible whitespace and indentation issues.
This script can be run to ensure that telegram_bot_db.py (or any Python file)
does not contain problematic Unicode whitespace characters.

Usage:
    python3 validate_whitespace.py [filename]
    
If no filename is provided, it defaults to 'telegram_bot_db.py'.
"""

import sys
import os

# List of problematic Unicode characters to detect
INVISIBLE_CHARS = {
    '\u200b': 'ZERO WIDTH SPACE',
    '\u200c': 'ZERO WIDTH NON-JOINER',
    '\u200d': 'ZERO WIDTH JOINER',
    '\u00a0': 'NON-BREAKING SPACE',
    '\u2002': 'EN SPACE',
    '\u2003': 'EM SPACE',
    '\u2004': 'THREE-PER-EM SPACE',
    '\u2005': 'FOUR-PER-EM SPACE',
    '\u2006': 'SIX-PER-EM SPACE',
    '\u2007': 'FIGURE SPACE',
    '\u2008': 'PUNCTUATION SPACE',
    '\u2009': 'THIN SPACE',
    '\u200a': 'HAIR SPACE',
    '\u202f': 'NARROW NO-BREAK SPACE',
    '\u205f': 'MEDIUM MATHEMATICAL SPACE',
    '\u3000': 'IDEOGRAPHIC SPACE',
    '\ufeff': 'ZERO WIDTH NO-BREAK SPACE (BOM)',
}


def check_invisible_whitespace(filename):
    """
    Check a file for invisible Unicode whitespace characters.
    
    Returns:
        tuple: (issues_found, total_issues)
        issues_found: list of dictionaries containing issue details
        total_issues: integer count of issues
    """
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' not found.")
        return [], 0
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file: {e}")
        return [], 0
    
    issues = []
    lines = content.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        for pos, char in enumerate(line):
            if char in INVISIBLE_CHARS:
                issues.append({
                    'line': line_num,
                    'column': pos + 1,
                    'char': char,
                    'name': INVISIBLE_CHARS[char],
                    'hex': hex(ord(char)),
                    'context': line[max(0, pos-10):min(len(line), pos+10)]
                })
    
    return issues, len(issues)


def check_tabs(filename):
    """
    Check a file for TAB characters in indentation.
    
    Returns:
        tuple: (tab_lines, total_tabs)
        tab_lines: list of line numbers containing tabs
        total_tabs: integer count of lines with tabs
    """
    if not os.path.exists(filename):
        return [], 0
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading file: {e}")
        return [], 0
    
    tab_lines = []
    
    for line_num, line in enumerate(lines, 1):
        if not line.strip():
            continue
        
        leading_ws = line[:len(line) - len(line.lstrip())]
        if '\t' in leading_ws:
            tab_lines.append(line_num)
    
    return tab_lines, len(tab_lines)


def validate_syntax(filename):
    """
    Validate that the Python file has correct syntax.
    
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            code = f.read()
        compile(code, filename, 'exec')
        return True, None
    except SyntaxError as e:
        return False, f"SyntaxError at line {e.lineno}: {e.msg}"
    except IndentationError as e:
        return False, f"IndentationError at line {e.lineno}: {e.msg}"
    except Exception as e:
        return False, f"Error: {e}"


def main():
    # Determine filename
    filename = sys.argv[1] if len(sys.argv) > 1 else 'telegram_bot_db.py'
    
    print("=" * 80)
    print(f"Validating whitespace in: {filename}")
    print("=" * 80)
    print()
    
    # Check for invisible whitespace
    print("1. Checking for invisible Unicode whitespace...")
    issues, total = check_invisible_whitespace(filename)
    
    if total > 0:
        print(f"   ❌ FAILED: Found {total} invisible whitespace character(s):")
        for issue in issues[:10]:  # Show first 10
            print(f"      Line {issue['line']}, Col {issue['column']}: "
                  f"{issue['name']} ({issue['hex']})")
        if total > 10:
            print(f"      ... and {total - 10} more")
    else:
        print("   ✅ PASSED: No invisible whitespace characters found")
    
    print()
    
    # Check for tabs
    print("2. Checking for TAB characters in indentation...")
    tab_lines, tab_count = check_tabs(filename)
    
    if tab_count > 0:
        print(f"   ⚠️  WARNING: Found TABs on {tab_count} line(s):")
        for line_num in tab_lines[:10]:  # Show first 10
            print(f"      Line {line_num}")
        if tab_count > 10:
            print(f"      ... and {tab_count - 10} more")
    else:
        print("   ✅ PASSED: No TAB characters found in indentation")
    
    print()
    
    # Check syntax
    print("3. Validating Python syntax...")
    is_valid, error = validate_syntax(filename)
    
    if is_valid:
        print("   ✅ PASSED: File has valid Python syntax")
    else:
        print(f"   ❌ FAILED: {error}")
    
    print()
    print("=" * 80)
    
    # Summary
    if total == 0 and tab_count == 0 and is_valid:
        print("✅ All checks passed! File is clean.")
        return 0
    else:
        print("⚠️  Some issues were found. Please review the output above.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
