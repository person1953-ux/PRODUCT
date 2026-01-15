import xml.etree.ElementTree as ET

# 1. Parse the XML file
try:
    tree = ET.parse('your_file.xml')
    # Get the root element of the XML tree
    root = tree.getroot()
except FileNotFoundError:
    print("Error: The file 'your_file.xml' was not found.")
    exit()
except ET.ParseError as e:
    print(f"Error parsing XML: {e}")
    exit()

# 2. Work with the XML data
# Print the tag of the root element
print(f"Root tag: {root.tag}")

##example of root element : bookstore
#<?xml version="1.0" encoding="UTF-8"?>
#<bookstore>
#  <book category="fiction">
#    <title lang="en">The Hitchhiker's Guide to the Galaxy</title>
#    <author>Douglas Adams</author>
#    <year>1979</year>
#    <price>12.99</price>
#  </book>
#</bookstore>

# Loop through all child elements of the root
for child in root:
    print(f"Child tag: {child.tag}, Attributes: {child.attrib}")

# Find a specific element using XPath
# Example: find the first element named 'item'
item = root.find('item')
if item is not None:
    print(f"Found item text: {item.text}")

# Find all elements with a specific tag
for item in root.findall('item'):
    name = item.find('name').text if item.find('name') is not None else 'N/A'
    price = item.find('price').text if item.find('price') is not None else 'N/A'
    print(f"Item Name: {name}, Price: {price}")