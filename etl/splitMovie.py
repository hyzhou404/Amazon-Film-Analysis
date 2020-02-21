import csv

movieReader = csv.reader(open("movies.csv", 'r', errors="ignore"))
movieWriter = csv.writer(open("dealed\\movies.csv", 'w', errors="ignore", newline=""))
directorWriter = csv.writer(open("dealed\\directors.csv", 'w', errors="ignore", newline=""))
actorWriter = csv.writer(open("dealed\\actors.csv", 'w', errors="ignore", newline=""))
studioWriter = csv.writer(open("dealed\\studios.csv", 'w', errors="ignore", newline=""))
formatWriter = csv.writer(open("dealed\\formats.csv", 'w', errors="ignore", newline=""))
languageWriter = csv.writer(open("dealed\\languages.csv", 'w', errors="ignore", newline=""))
genreWriter = csv.writer(open("dealed\\genres.csv", 'w', errors="ignore", newline=""))
directWriter = csv.writer(open("dealed\\direct.csv", 'w', errors="ignore", newline=""))
actWriter = csv.writer(open("dealed\\act.csv", 'w', errors="ignore", newline=""))
produceWriter = csv.writer(open("dealed\\produce.csv", 'w', errors="ignore", newline=""))
hasEditionWriter = csv.writer(open("dealed\\hasEdition.csv", 'w', errors="ignore", newline=""))
hasLanguageWriter = csv.writer(open("dealed\\hasLanguage.csv", 'w', errors="ignore", newline=""))
hasGenreWriter = csv.writer(open("dealed\\hasGenre.csv", 'w', errors="ignore", newline=""))
directorId = 0
actorId = 0
studioId = 0
formatId = 0
languageId = 0
genreId = 0
directorMap = {}
actorMap = {}
studioMap = {}
formatMap = {}
languageMap = {}
genreMap = {}

rankMap = {}
# put ranks into dictionary
rankReader = csv.reader(open("rank.csv", errors="ignore"))
for line in rankReader:
    if line[0] not in rankMap:
        rankMap[line[0]] = [float(line[1])]
    else:
        rankMap[line[0]].append(float(line[1]))

for line in movieReader:
    if int(line[0])%10000 == 0:
        print(line[0])
    # get avg_score and review_num
    if line[10] in rankMap:
        ranks = rankMap[line[10]]
        review_num = len(ranks)
        avg_score = sum(ranks)/review_num
    else:
        review_num = 0
        avg_score = 0

    # MOVIE
    # id, name, ASIN, release_time, avg_score, review_num
    line[0] = line[0].replace("\n", " ").strip()
    line[8] = line[8].replace("\n", " ").strip()
    line[10] = line[10].replace("\n", " ").strip()
    line[9] = line[9].replace("\n", " ").strip()
    movieline = [line[0], line[8], line[10], line[9], avg_score, review_num]
    movieWriter.writerow(movieline)

    # DIRECTOR & DIRECT
    # id, name
    # directorId, movieId
    directors = line[3].split(",")
    for director in directors:
        director = director.strip()
        director = director.replace("\n", " ")
        if director != "":
            if director not in directorMap:
                directorMap[director] = directorId
                directorline = [directorId, director]
                directorWriter.writerow(directorline)
                directorId += 1
            directline = [directorMap[director], line[0]]
            directWriter.writerow(directline)

    # ACTOR & ACT
    # id, name
    # actorId, movieId, isStar
    starActors = set(line[1].split(","))
    supportActors = set(line[6].split(","))
    supportActors = supportActors - starActors
    for actor in starActors:
        actor = actor.strip()
        actor = actor.replace("\n", " ")
        if actor != "":
            if actor not in actorMap:
                actorMap[actor] = actorId
                actorline = [actorId, actor]
                actorWriter.writerow(actorline)
                actorId += 1
            actline = [actorMap[actor], line[0], True]
            actWriter.writerow(actline)
    for actor in supportActors:
        actor = actor.strip()
        actor = actor.replace("\n", " ")
        if actor != "":
            if actor not in actorMap:
                actorMap[actor] = actorId
                actorline = [actorId, actor]
                actorWriter.writerow(actorline)
                actorId += 1
            actline = [actorMap[actor], line[0], False]
            actWriter.writerow(actline)

    # STUDIO & PRODUCE
    # id, name
    # studioId, movieId
    studios = line[7].split(",")
    for studio in studios:
        studio = studio.strip()
        studio = studio.replace("\n", " ")
        if studio != "":
            if studio not in studioMap:
                studioMap[studio] = studioId
                studioline = [studioId, studio]
                studioWriter.writerow(studioline)
                studioId += 1
            produceline = [studioMap[studio], line[0]]
            produceWriter.writerow(produceline)

    # FORMAT & HAS_EDITION
    # formatName
    # movieId, formatName
    formats = line[2].split(",")
    for f in formats:
        f = f.strip()
        f = f.replace("\n", " ")
        if f != "":
            if f not in formatMap:
                formatMap[f] = formatId
                formatWriter.writerow([f])
                formatId += 1
            hasEditionline = [line[0], f]
            hasEditionWriter.writerow(hasEditionline)

    # LANGUAGE & HAS_LANGUAGE
    # languageName
    # movieId, languageName
    languages = line[5].split(",")
    for language in languages:
        language = language.strip()
        language = language.replace("\n", " ")
        if language != "":
            if language not in languageMap:
                languageMap[language] = languageId
                languageWriter.writerow([language])
                languageId += 1
            hasLanguageline = [line[0], language]
            hasLanguageWriter.writerow(hasLanguageline)

    # GENRE & HAS_GENRE
    # GenreName
    # movieId, GenreName
    genres = line[4].split(",")
    for genre in genres:
        genre = genre.strip()
        genre = genre.replace("\n", " ")
        if genre != "":
            if genre not in genreMap:
                genreMap[genre] = genreId
                genreWriter.writerow([genre])
                genreId += 1
            hasGenreline = [line[0], genre]
            hasGenreWriter.writerow(hasGenreline)



