library(RMySQL) 
library(RSQLite)
library(DBI)

xmlFile <- "pubmed-tfm-xml/pubmed22n0001-tfTEMP.xml" #At the top of the file so it can be found and changed easily
dbFile <- "Publications.db"
dbcon <- dbConnect(RSQLite::SQLite(), dbFile)

db_user <- 'root'
db_password <- 'banana123'
db_name <- 'PracticumTwo'
db_host <- 'localhost' 
db_port <- 3306
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

#Notes about the table: 
  #This table should have the answer for every question in the assignment in either a given row or a group of rows.
#This table cannot answer a few questions that are similar to those asked, such as "How many unique authors published articles between 2012 and 2015" 
#(It will have the data for each of those years individually, but no way to get the total unique authors for a combination of years as the table does not store data on individual authors),
#as that is not a question in the assignment and would take significant resources to accommadate.
  #Because of the various queries, I use a year of -1 to represent data across all years.  
#For instance, the number of unique articles for a journal in March across all years would be found in the NumArticles element where the Year has a value of -1 and the Month has a value of 3.
  #Because of the need to also be able to sort by quarter, and due to the fact that quarters, like months,
#are fractions of individual years, quarters 1-4 are represented by Month values 13-16.
  #Additionally, a Month value of -1 represents the data across all months, similar to a value of -1 for Year 
#(The number of unique articles in 2012 across all months is found in the NumArticles element where the Year has a value of 2012 and the Month has a value of -1).
#I later assume that all journals are published from 1975-2030, which works for the given dataset and could be changed if necessary.  This could be easily changed if new journals were to be added with a publication year outside this range, and makes the table more convenient to use.
dbGetQuery(mydb,"DROP TABLE IF EXISTS FactTable")
dbGetQuery(mydb,"CREATE TABLE FactTable (ID INTEGER PRIMARY KEY, JournalTitle TEXT, Year INTEGER, Month INTEGER, NumArticles INTEGER, NumUniqueAuthors INTEGER)")
FactTable.df <- data.frame (ID = integer(), #PK
                            JournalTitle = character(), #This is the journal PK
                            Year = integer(), 
                            Month = integer(),
                            NumArticles = integer(), 
                            NumUniqueAuthors = integer(),
                            stringsAsFactors = F)


#Need all data from Journals table, some data from Articles and ArticleAuthors tables, and no data from Authors table

JournalsData <- dbGetQuery(dbcon,"SELECT
   `Title`
  FROM Journals")

ArticlesData <- dbGetQuery(dbcon,"SELECT
   `PMID`, 
   `JournalTitle`, 
   `Year`,
   `Month`
  FROM Articles")

ArticleAuthorsData <- dbGetQuery(dbcon,"SELECT
   `PMID`, 
   `AID`
  FROM ArticleAuthors")


id = 1
numJournals = nrow(JournalsData)
validYears = c(-1, 1975:2030)
validMonths = c(-1, 1:16)


#This is used for temporary storage to allow adding multiple rows to the main dataframe at once because adding every row individually takes an exceptionally long time
TempFactTable.df <- data.frame (ID = integer(), 
                                JournalTitle = character(), 
                                Year = integer(), 
                                Month = integer(),
                                NumArticles = integer(), 
                                NumUniqueAuthors = integer(),
                                stringsAsFactors = F)



for (i in 1:numJournals) {
  #Need to fill every combination of year from 1975-2030 and -1 with every combination of month from 1-16 and -1
  currentTitle = JournalsData[i, "Title"]
  relevantArticles = ArticlesData[which(ArticlesData$JournalTitle == currentTitle),]
  relevantAuthors = ArticleAuthorsData[which(ArticleAuthorsData$PMID == relevantArticles[1, "PMID"]),]
  for (a in 1:nrow(relevantArticles)) {
    if (a != 1) {
      relevantAuthors = rbind(relevantAuthors, ArticleAuthorsData[which(ArticleAuthorsData$PMID == relevantArticles[a, "PMID"]),])
    }
  }
  for (j in validYears) {
    k = -1
    if (j == -1) {
      #All years, all months
      exactArticles = relevantArticles
      numArticles = nrow(exactArticles)
      numAuthors = 0
      if (numArticles > 0) {
        exactAuthors = relevantAuthors
        exactAuthors = exactAuthors[!duplicated(exactAuthors$AID),]
        numAuthors = nrow(exactAuthors)
      }
      TempFactTable.df[nrow(TempFactTable.df) + 1,] = list(id, currentTitle, j, k, numArticles, numAuthors)
      id = id + 1
    } else {
      #Year j, all months
      exactArticles = relevantArticles
      exactArticles = exactArticles[which(exactArticles$Year == j),]
      numArticles = nrow(exactArticles)
      numAuthors = 0
      if (numArticles > 0) {
        exactAuthors = relevantAuthors
        exactAuthors = exactAuthors[!duplicated(exactAuthors$AID),]
        numAuthors = nrow(exactAuthors)
      }
      TempFactTable.df[nrow(TempFactTable.df) + 1,] = list(id, currentTitle, j, k, numArticles, numAuthors)
      id = id + 1
    }
    for (k in 1:12) {
      if (j == -1) {
        #All years, month k
        exactArticles = relevantArticles[which(relevantArticles$Month == k),]
        numArticles = nrow(exactArticles)
        numAuthors = 0
        if (numArticles > 0) {
          exactAuthors = relevantAuthors[which(relevantAuthors$PMID == exactArticles[1, "PMID"]),]
          for (a in 1:nrow(exactArticles)) {
            if (a != 1) {
              exactAuthors = rbind(exactAuthors, relevantAuthors[which(relevantAuthors$PMID == exactArticles[a, "PMID"]),])
            }
          }
          exactAuthors = exactAuthors[!duplicated(exactAuthors$AID),]
          numAuthors = nrow(exactAuthors)
        }
        TempFactTable.df[nrow(TempFactTable.df) + 1,] = list(id, currentTitle, j, k, numArticles, numAuthors)
        id = id + 1
      } else {
        #Year j, month k
        exactArticles = relevantArticles[which(relevantArticles$Month == k),]
        exactArticles = exactArticles[which(exactArticles$Year == j),]
        numArticles = nrow(exactArticles)
        numAuthors = 0
        if (numArticles > 0) {
          exactAuthors = relevantAuthors[which(relevantAuthors$PMID == exactArticles[1, "PMID"]),]
          for (a in 1:nrow(exactArticles)) {
            if (a != 1) {
              exactAuthors = rbind(exactAuthors, relevantAuthors[which(relevantAuthors$PMID == exactArticles[a, "PMID"]),])
            }
          }
          exactAuthors = exactAuthors[!duplicated(exactAuthors$AID),]
          numAuthors = nrow(exactAuthors)
        }
        TempFactTable.df[nrow(TempFactTable.df) + 1,] = list(id, currentTitle, j, k, numArticles, numAuthors) #Optimize somehow
        id = id + 1
      }
    }
    for (k in 13:16) {
      if (j == -1) {
        #All years, quarter k minus 12
        exactArticles = relevantArticles[which(relevantArticles$Month == (((k - 12) * 3) - 2)),]
        exactArticles = rbind(exactArticles, relevantArticles[which(relevantArticles$Month == (((k - 12) * 3) - 1)),])
        exactArticles = rbind(exactArticles, relevantArticles[which(relevantArticles$Month == ((k - 12) * 3)),])
        numArticles = nrow(exactArticles)
        numAuthors = 0
        if (numArticles > 0) {
          exactAuthors = relevantAuthors[which(relevantAuthors$PMID == exactArticles[1, "PMID"]),]
          for (a in 1:nrow(exactArticles)) {
            if (a != 1) {
              exactAuthors = rbind(exactAuthors, relevantAuthors[which(relevantAuthors$PMID == exactArticles[a, "PMID"]),])
            }
          }
          exactAuthors = exactAuthors[!duplicated(exactAuthors$AID),]
          numAuthors = nrow(exactAuthors)
        }
        TempFactTable.df[nrow(TempFactTable.df) + 1,] = list(id, currentTitle, j, k, numArticles, numAuthors)
        id = id + 1
      } else {
        #Year j, quarter k minus 12
        exactArticles = relevantArticles[which(relevantArticles$Month == (((k - 12) * 3) - 2)),]
        exactArticles = rbind(exactArticles, relevantArticles[which(relevantArticles$Month == (((k - 12) * 3) - 1)),])
        exactArticles = rbind(exactArticles, relevantArticles[which(relevantArticles$Month == ((k - 12) * 3)),])
        exactArticles = exactArticles[which(exactArticles$Year == j),]
        numArticles = nrow(exactArticles)
        numAuthors = 0
        if (numArticles > 0) {
          exactAuthors = relevantAuthors[which(relevantAuthors$PMID == exactArticles[1, "PMID"]),]
          for (a in 1:nrow(exactArticles)) {
            if (a != 1) {
              exactAuthors = rbind(exactAuthors, relevantAuthors[which(relevantAuthors$PMID == exactArticles[a, "PMID"]),])
            }
          }
          exactAuthors = exactAuthors[!duplicated(exactAuthors$AID),]
          numAuthors = nrow(exactAuthors)
        }
        TempFactTable.df[nrow(TempFactTable.df) + 1,] = list(id, currentTitle, j, k, numArticles, numAuthors)
        id = id + 1
      }
    }
  }
  if (i %% 4 == 0) {
    FactTable.df = rbind(FactTable.df, TempFactTable.df)
    TempFactTable.df <- data.frame (ID = integer(), 
                                    JournalTitle = character(), 
                                    Year = integer(), 
                                    Month = integer(),
                                    NumArticles = integer(), 
                                    NumUniqueAuthors = integer(),
                                    stringsAsFactors = F)
  }
}
FactTable.df = rbind(FactTable.df, TempFactTable.df)


dbWriteTable(mydb, "FactTable", FactTable.df, append = TRUE, row.names = FALSE)

dbDisconnect(dbcon)
dbDisconnect(mydb)
