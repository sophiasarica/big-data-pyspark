

from mrjob.job import MRJob
from nltk.corpus import words
setofwords = set(words.words())
import re


def check_eng(word):
    if word == '' or not word:
        return False
    word = word.strip().lower()
    word = re.sub(r'[^\w\s]', '', word)
    return word in setofwords

class Count(MRJob):
    """ The below mapper() function defines the mapper for MapReduce and takes 
    key value argument and generates the output in tuple format . 
    The mapper below is splitting the line and generating a word with its own 
    count i.e. 1 """
    def mapper(self, _, line):
         for word in line.split():
            result = check_eng(word)
            if result == False:
               yield(word, 1)

    """ The below reducer() is aggregating the result according to their key and
    producing the output in a key-value format with its total count"""        
    def reducer(self, word, counts):
         yield(word, sum(counts))
  
"""the below 2 lines are ensuring the execution of mrjob, the program will not
execute without them"""        

Count.run()





