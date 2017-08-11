library(httr)
library(solrium)

globalSolrHost <- 'localhost'
globalSolrPort <- 8983
globalCollection <- 'gettingstarted'


# add random data to the collection. it is expected that Solr will generate IDs automatically
fFill <- function(count=3000, batch=50000) {
  solr_connect(url = paste0('http://', globalSolrHost, ":", globalSolrPort))
  i <- 0
  totalTime <- 0
  n <- sample (1:batch)
  while (i<count) {
    df <- data.frame(name = n)
    t <- fGetTime()
    add(df, name = globalCollection, commit = TRUE)
    t <- fGetTime() - t
    i <- i + 1
    totalTime <- totalTime + t
    message(paste0("round ", i, "/", count, ", ", batch, " events in ", t, " seconds, total run for fFill: ", totalTime, " seconds")) 
  }
}

# query solr using httr
fNoPaging <- function (rows=0, start=0, sort=NULL, cursorMark=NULL, q="*:*", 
                       solrHost=globalSolrHost, solrPort=globalSolrPort, collection=globalCollection) {

  rows <- format(rows, scientific = FALSE)
  start <- format(start, scientific = FALSE)
  solrUrl <- paste0("http://", globalSolrHost, ":", globalSolrPort, "/solr/", globalCollection, "/query")
  postContent <- list(q = q, rows = rows, start = start)
  if (! is.null(sort)) {
    postContent <- c(postContent, sort = sort)
  }
  if (! is.null(cursorMark)) {
    postContent <- c(postContent, cursorMark = cursorMark)
  }
  r < -POST(solrUrl, body = postContent, encode = "form")
  return(fParser(r))
}

# query solr with paging/deep paging with page of size 50000 rows
fPaging <- function(page=50000, rows=7851281, sort=NULL) {
  t <- fGetTime()
  numOfPages <- rows %/% page
  numFound <- rows
  for (pageNum in 0:numOfPages) {
      pagingStart <- as.integer(pageNum * page)
      if ( (pagingStart + page) > numFound) rows <- numFound %% page else rows <- page
      result <- fNoPaging(rows, pagingStart)
  }
  t <- fGetTime() - t
  message(t)
}

# query solr using cursors
fCursor <- function(page=50000, rows=7851281, sort="id asc") {
  t <- fGetTime()
  cursorMark <- "*"
  while(rows>0) {
    if (rows < page) page <- rows
    result <- fNoPaging(page, 0, sort, cursorMark)
    rows <- rows - page
    if (cursorMark == result$nextCursorMark) {
      rows <- 0
    } else {
      cursorMark <- result$nextCursorMark
    }
  }
  t <- fGetTime() - t
  message(t)
}

# if the output of httr is being parsed or not
fParser <- function(r, parse=TRUE) {
  if (parse) {
    return (httr::content(r, "parsed", "application/json"))
  } else {
    return (r)
  }
}

# format time to measure time
fGetTime <- function(t=Sys.time(), format="%s") {
  return (as.integer(format(t, format)))
}



## main ##

# enable if random data needs to be added. fFill depends on solrium library.
# fFill(count=10, batch=50000)

globalRows <- fNoPaging(rows=0)$response$numFound
fPaging(rows=10)
fPaging(rows=globalRows)
fPaging(rows=globalRows, sort="id asc")
fCursor(rows=globalRows)


