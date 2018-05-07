archive = spark.read.json("/home/rennan/Documents/latin/kworb/arquivos/final/history.out.json", multiLine = True)
archiveRDD = archive.rdd

#Limpeza dos dados

def f(x):
	x[2] = x[2].split("(")[0]s
	return x


#Plota todas as vezes que os artista aparecem, seja sozinhos, ou em feats (antes os feats era considerados um artista apenas)
artists = archiveRDD.flatMap(lambda x: x[0].split(","))
artists_frequence = artists.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)


#Plota todas as relações musica-artista e aparecem mais de uma vez, caso seja uma música frequente
artist_music = archiveRDD.map(lambda x: (x[0].split(",")[0],x[2].split(" (")[0]))

#Plota todas as relações musica-posicao e aparecem mais de uma vez, caso seja uma música frequente
music_pos = archiveRDD.map(lambda x: (x[2].split(" (")[0], x[1]))

#Todas as musica:
musics = archiveRDD.map(lambda x: x[2].split(" (")[0].lower())
frequence_musics = musics.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

#Todas as palavras presentes em todas as músicas
music_words = musics.flatMap(lambda x: x.split())
frequence_words = music_words.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
frequence_words5 = frequence_words.filter(lambda x: len(x[0])>=2)