import re
with open("results.txt") as f:
    t = f.read().strip()

lists = []
m = re.findall(r"\[([\d\",]*)\]", t) # Package Codes
# m = re.findall(r"Some product code could not be parsed:([^.]*)", t) # Product Codes
for x in m:
    lists.append(x)

for item in lists:
    print(item)