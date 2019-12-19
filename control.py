import hashlib

hashes = []

for i in range(0, 5000000):
    if i % 1000000 == 0:
        print(i)
    m = hashlib.sha256()
    m.update(str(i))
    hashes.append(m.digest())

hashes.sort()
m = hashlib.sha256()
for item in hashes:
    m.update(item)
print(m.hexdigest())
