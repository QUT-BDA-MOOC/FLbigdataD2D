-- Big Data: from Data to Decisions
-- https://www.futurelearn.com/courses/big-data-decisions
--

-- Load the input file and call the single field in the record 'line'
in = load 'prince/price_by_machiavelli.txt' as (line);

-- TOKENIZE --> splits the line into a field for each word
-- flatten(TOKENIZE(...)) - create collection of word(s)
words = foreach in generate flatten(TOKENIZE(line)) as word;

-- Print out the words
dump words;

-- Group records by word
grouped  = group words by word;

-- Count words
out  = foreach grouped generate group, COUNT(words);

-- Print out the results
dump out;