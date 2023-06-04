# Basic instructions for using the project

```java
Path path = Files.createTempDirectory("storagedb");

int valueSize = 28;
StorageDB db = new StorageDBBuilder()
        .withDbDir(path.toString())
        .withValueSize(valueSize)
        .withAutoCompactDisabled()
        .build();


ByteBuffer value = ByteBuffer.allocate(8);
int totalRecords = 100_000;
for (int j = 0; j < totalRecords; j++) {
        db.put(j, value.array());
}


byte[] storedValueBytes = db.randomGet(15);


db.iterate((key, data, offset) -> {
    ByteBuffer value = ByteBuffer.wrap(data, offset, valueSize);
        System.out.println(keyValue.get(key), value.getInt());
});

db.close();
```