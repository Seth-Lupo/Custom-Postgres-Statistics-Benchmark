file = open("Outputs/StarbucksTest2/StarbucksTest28.csv", "r")
lines = ""
for line in file:
    lines += line
lines2 = lines[:-1]
file2 = open("Outputs/StarbucksTest2/StarbucksTest28-2.csv", "w")
file2.write(lines2)
file2.close()
file.close()