archive = spark.read.json("/home/rennan/Documents/latin/kworb/arquivos/final/history.out.json", multiLine = True)
archiveRDD = archive.rdd

#Limpeza dos dados

def f(x):
	x[2] = x[2].split("(")[0]s
	return x


#Todas os artisas presentes (e em ordem)
artists = archiveRDD.flatMap(lambda x: x[0].split(","))
artists = artists.map(lambda x: x.lower())
artists_frequence = artists.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

#Todas as musica:
musics = archiveRDD.map(lambda x: x[2].split(" (")[0].lower())
frequence_musics = musics.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

#Todas as palavras presentes em todas as músicas
music_words = musics.flatMap(lambda x: x.split())
frequence_words = music_words.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

#Todas as palavras com ou mais de 5/10 letras presentes em todas as músicas
music_words5 = music_words.filter(lambda x: len(x)>=5)
frequence_words5 = music_words5.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

music_words10 = music_words.filter(lambda x: len(x)>=10)
frequence_words10 = music_words10.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

#Todas musicas que mais ficaram em primeiro lugar todas as músicas
music_pos = archiveRDD.map(lambda x: (x[2].split(" (")[0], x[1]))
music_1 = music_pos.filter(lambda x: x[1] == '1')
music_1_name = music_1.map(lambda x: x[0])
frequence_musics_1 = music_1_name.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)


#Todas os artistas que mais ficaram em primeiro lugar
artist_pos = archiveRDD.map(lambda x: (x[0].split(",")[0], x[1]))
artist_1 = artist_pos.filter(lambda x: x[1] == '1')
artist_1_name = artist_1.map(lambda x: x[0])
frequence_artist_1 = artist_1_name.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

#Todas as palavras mais frequentes das musicas que ficaram em primeiro lugar
#Todas as duplas de palavras presentes em todas as músicas

#Plota todas as relações musica-artista e aparecem mais de uma vez, caso seja uma música frequente
artist_music = archiveRDD.map(lambda x: (x[0].split(",")[0],x[2].split(" (")[0]))

