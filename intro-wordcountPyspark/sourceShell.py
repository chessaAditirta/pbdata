text_file = sc.textFile("coba.txt")
counts = text_file.flatMap(lambda line: line.split(" "))  .map(lambda word: (word, 1))  .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hasil")