"""Check CORPCODE.xml structure"""

import xml.etree.ElementTree as ET
from pathlib import Path

xml_path = Path(__file__).parent / "CORPCODE.xml"

# Parse XML
tree = ET.parse(xml_path)
root = tree.getroot()

print(f"Root tag: {root.tag}")
print(f"Root attributes: {root.attrib}")

# Get first few corp entries
corps = root.findall('.//list')[:5]

print(f"\nTotal corporations: {len(root.findall('.//list'))}")
print("\nFirst corporation structure:")

if corps:
    first_corp = corps[0]
    for child in first_corp:
        print(f"  {child.tag}: {child.text}")
    
    print("\n\nAll available tags:")
    all_tags = set()
    for corp in root.findall('.//list')[:100]:  # Check first 100
        for child in corp:
            all_tags.add(child.tag)
    
    print(sorted(all_tags))
