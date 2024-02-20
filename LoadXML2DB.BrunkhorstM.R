library(XML)
library(RSQLite)
library(DBI)

xmlFile <- "pubmed-tfm-xml/pubmed22n0001-tf.xml" #At the top of the file so it can be found and changed easily
dbFile <- "Publications.db"
dbcon <- dbConnect(RSQLite::SQLite(), dbFile)

#Part 1 Section 4 says to only store the data that is necessary, so not all data is stored

#PMID appears to be unique and can be used as a primary key
Articles.df <- data.frame (PMID = integer(), #PK
                           JournalTitle = character(), #FK
                           ArticleTitle = character(), 
                           Year = integer(), 
                           Month = integer(), #Needs conversion
                           stringsAsFactors = F)

#Name of authors is stored to check uniqueness if table needs to be expanded later
#The data given doesn't have a better way to check author uniqueness from what I can tell
Authors.df <- data.frame (AID = integer(), #Synthetic PK
                          LastName = character(), 
                          ForeName = character(),
                          stringsAsFactors = F)

#Assuming titles are unique and can be used as primary key
#This table doesn't feel necessary but part 1 section 4 of the assignment says there should be a Journals table, I don't see any other necessary info for it
Journals.df <- data.frame (Title = character(), #PK
                           stringsAsFactors = F)

ArticleAuthors.df <- data.frame (AAID = integer(), #PK
                                 PMID = integer(), #FK
                                 AID = integer(), #FK
                                 stringsAsFactors = F)

dom <- xmlParse(xmlFile, validate=T)
publications <- xmlRoot(dom)
numArticles = xmlSize(publications)

#These are used for temporary storage to allow adding multiple rows to the main dataframes at once because adding every row individually takes an exceptionally long time
TempArticles.df <- data.frame (PMID = integer(), 
                               JournalTitle = character(), 
                               ArticleTitle = character(), 
                               Year = integer(), 
                               Month = integer(), 
                               stringsAsFactors = F)

TempAuthors.df <- data.frame (AID = integer(), 
                              LastName = character(), 
                              ForeName = character(),
                              stringsAsFactors = F)

TempJournals.df <- data.frame (Title = character(), 
                               stringsAsFactors = F)

TempArticleAuthors.df <- data.frame (AAID = integer(), 
                                     PMID = integer(), 
                                     AID = integer(), 
                                     stringsAsFactors = F)


getMonth <- function(Month) {
  if (Month == "Jan") {
    return(1)
  } else if (Month == "Feb") {
    return(2)
  } else if (Month == "Mar") {
    return(3)
  } else if (Month == "Apr") {
    return(4)
  } else if (Month == "May") {
    return(5)
  } else if (Month == "Jun") {
    return(6)
  } else if (Month == "Jul") {
    return(7)
  } else if (Month == "Aug") {
    return(8)
  } else if (Month == "Sep") {
    return(9)
  } else if (Month == "Oct") {
    return(10)
  } else if (Month == "Nov") {
    return(11)
  } else if (Month == "Dec") {
    return(12)
  }
  return(-2) #Should only happen if month is improperly formatted in xml file
}

numJournals = 0
numAuthors = 0
AAID = 0
for (i in 1:numArticles) {
  article <- publications[[i]]
  pubDetails <- article[[1]]
  journal <- pubDetails[[1]]
  journalIssue <- journal[[2]]
  pubDate <- journalIssue[[3]]
  #All of the following if statements are needed because the XML does not have a consistent location and format for its dates
  if (is.na(xmlValue(pubDate[[1]]))) {
    pubDate <- journalIssue[[2]]
    if (is.na(xmlValue(pubDate[[1]]))) {
      pubDate <- journalIssue[[1]]
      if (is.na(xmlValue(pubDate[[1]]))) {
        journalIssue <- journal[[1]]
        pubDate <- journalIssue[[3]]
        if (is.na(xmlValue(pubDate[[1]]))) {
          pubDate <- journalIssue[[2]]
          if (is.na(xmlValue(pubDate[[1]]))) {
            pubDate <- journalIssue[[1]]
          }
        }
      }
    }
  }
  authorList <- pubDetails[[3]]
  
  PMID = xmlAttrs(article)
  PMID = strtoi(PMID)
  JournalTitle = xmlValue(journal[[3]])
  ArticleTitle = xmlValue(pubDetails[[2]])
  Year = xmlValue(pubDate[[1]])
  if (nchar(Year) > 4) {
    Year = substr(Year, 1, 4)
  }
  Year = strtoi(Year)
  if (xmlSize(pubDate) > 1) {
    Month = xmlValue(pubDate[[2]]) #Needs to be converted to a number, I don't see a better way than a bunch of if-else statements
    Month = getMonth(Month)
  } else { 
    #Happens for medlinedates (which contain multiple months) and articles with no specified month
    #Articles with Month < 0 and their data are not shown later when sorting by month or quarter, but are shown when sorting by year
    Month = -1
  }
  TempArticles.df[nrow(TempArticles.df) + 1,] = list(PMID, JournalTitle, ArticleTitle, Year, Month)
  
  authorListSize = xmlSize(authorList) 
  for (j in 1:authorListSize) {
    author <- authorList[[j]]
    LastName = xmlValue(author[[1]])
    ForeName = xmlValue(author[[2]])
    AID = (TempAuthors.df[which(TempAuthors.df$LastName == LastName, TempAuthors.df$ForeName == ForeName), 1])[1]
    if (is.na(AID)) {
      AID = (Authors.df[which(Authors.df$LastName == LastName, Authors.df$ForeName == ForeName), 1])[1]
    }
    if (is.na(AID)) {
      AID = -1
    }
    if (AID == -1) {
      numAuthors = numAuthors + 1 #increment first so that the value for AID starts at 1 instead of 0 to match PMID
      AID = numAuthors
      TempAuthors.df[nrow(TempAuthors.df) + 1,] = list(AID, LastName, ForeName)
    }
    AAID = AAID + 1
    TempArticleAuthors.df[nrow(TempArticleAuthors.df) + 1,] = list(AAID, PMID, AID)
  }
  
  if (!(any(TempJournals.df == JournalTitle, na.rm = TRUE))) {
    if (!(any(Journals.df == JournalTitle, na.rm = TRUE))) {
      TempJournals.df[nrow(TempJournals.df) + 1,] = list(JournalTitle)
    }
  }
  if (i%%200 == 0) {
    Articles.df = rbind(Articles.df, TempArticles.df)
    Authors.df = rbind(Authors.df, TempAuthors.df)
    Journals.df = rbind(Journals.df, TempJournals.df)
    ArticleAuthors.df = rbind(ArticleAuthors.df, TempArticleAuthors.df)
    TempArticles.df <- data.frame (PMID = integer(), 
                               JournalTitle = character(), 
                               ArticleTitle = character(), 
                               Year = integer(), 
                               Month = integer(), 
                               stringsAsFactors = F)
    
    TempAuthors.df <- data.frame (AID = integer(), 
                              LastName = character(), 
                              ForeName = character(),
                              stringsAsFactors = F)
    
    TempJournals.df <- data.frame (Title = character(), 
                               stringsAsFactors = F)
    
    TempArticleAuthors.df <- data.frame (AAID = integer(), 
                                     PMID = integer(), 
                                     AID = integer(), 
                                     stringsAsFactors = F)
  }
}
Articles.df = rbind(Articles.df, TempArticles.df)
Authors.df = rbind(Authors.df, TempAuthors.df)
Journals.df = rbind(Journals.df, TempJournals.df)
ArticleAuthors.df = rbind(ArticleAuthors.df, TempArticleAuthors.df)



dbExecute(dbcon, "DROP TABLE IF EXISTS Journals")
dbExecute(dbcon, "
CREATE TABLE Journals
(
  Title TEXT PRIMARY KEY
)")
          
dbExecute(dbcon, "DROP TABLE IF EXISTS Articles")
dbExecute(dbcon, "
CREATE TABLE Articles
(
  PMID INTEGER PRIMARY KEY, 
  JournalTitle TEXT,
  ArticleTitle TEXT,
  Year INTEGER,
  Month INTEGER,
  FOREIGN KEY (JournalTitle) REFERENCES Journals (Title)
)")
                    
dbExecute(dbcon, "DROP TABLE IF EXISTS Authors")
dbExecute(dbcon, "
CREATE TABLE Authors
(
  AID INTEGER PRIMARY KEY, 
  LastName TEXT,
  ForeName TEXT
)")
                              
dbExecute(dbcon, "DROP TABLE IF EXISTS ArticleAuthors")
dbExecute(dbcon, "
CREATE TABLE ArticleAuthors
(
  AAID INTEGER PRIMARY KEY, 
  PMID INTEGER,
  AID INTEGER,
  FOREIGN KEY (PMID) REFERENCES Articles (PMID),
  FOREIGN KEY (AID) REFERENCES Authors (AID)
)")


dbWriteTable(dbcon, "Articles", Articles.df, append = TRUE, row.names = FALSE)
dbWriteTable(dbcon, "Authors", Authors.df, append = TRUE, row.names = FALSE)
dbWriteTable(dbcon, "Journals", Journals.df, append = TRUE, row.names = FALSE)
dbWriteTable(dbcon, "ArticleAuthors", ArticleAuthors.df, append = TRUE, row.names = FALSE)

dbDisconnect(dbcon)
