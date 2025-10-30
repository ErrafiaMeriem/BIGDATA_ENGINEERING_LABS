import sys
# input comes from STDIN
for line in sys.stdin:
    line = line.strip() 
    words = line.split() #split the line into words and returns as a list
    for word in words:
         #write the results to standard output STDOUT
        print('%s\t%s' % (word,1))