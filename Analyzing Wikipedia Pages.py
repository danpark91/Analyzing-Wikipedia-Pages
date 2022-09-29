#!/usr/bin/env python
# coding: utf-8

# # Analyzing Wikipedia Pages

# In this project, we'll work with data scraped from Wikipedia. Volunteer content contributors and editors maintain Wikipeda by continuously improving content. Anyone can edit Wikipedia, and because Wikipedia is crowdsourced, it has rapidly assembed a huge library of articles.
# 
# We'll implement a simplified version of the grep command-line utility to search for data in 54 megabytes worth of articles. The grep command and the grep utility essentially allows searching for textual data in all files from a given directory.
# 
# Articles were saved using the last component of their URLs. A page on Wikipedia has the URL structure https://en.wikipedia.org/wiki/Yarkant_County. When saving the article with the URL provided, we'd save it to the file Yarkant_County.html. All the data files are located in the wiki folder. Note that the files are in raw HTML, but we can treat the files like plain-text.
# 
# Our main goals will be the following:
# - Search for all occurrences of a string in all of the files.
# - Provide a case-insensitive option to the search.
# - Refine the result by providing the specific locations of the files.

# ## List all files in the wiki folder
# We can create a list with the names of all files in the wiki folder using the os.listdir() function.

# In[67]:


import os

file_names = os.listdir("wiki")
len(file_names)


# ## Read the first file
# Let's read the first file and print its contents. We need to join the name of the file with the wiki folder. We can do this using the os.path.join() function.

# In[68]:


with open(os.path.join("wiki", file_names[0])) as f:
    print(f.read())


# ## Adding the MapReduce function to this project
# We start by adding the MapReduce function so that we can use it throughout the project. We explore the data a little bit more and count the total number of lines in all files stored in the wiki folder using the MapReduce function.

# In[69]:


import math
import functools
from multiprocessing import Pool

def make_chunks(data, num_chunks):
    chunk_size = math.ceil(len(data) / num_chunks)
    return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

def map_reduce(data, num_processes, mapper, reducer):
    chunks = make_chunks(data, num_processes)
    pool = Pool(num_processes)
    chunk_results = pool.map(mapper, chunks)
    pool.close()
    pool.join()
    return functools.reduce(reducer, chunk_results)


# ## Counting the total number of lines on all files
# We will count the total number of lines on all files in the wiki folder using the MapReduce function.

# In[70]:


def map_line_count(file_names):
    total = 0
    for fn in file_names:
        with open(os.path.join("wiki", fn)) as f:
            total += len(f.readlines())
    return total
    
def reduce_line_count(count1, count2):
    return count1 + count2

target = "data"
map_reduce(file_names, 8, map_line_count, reduce_line_count)


# ## Grep string function
# A mapreduce_grep_string() function was defined that takes two arguments as input:
# 1. A path to the folder. We will use it on the wiki folder but having the argument makes the function easier to reuse.
# 2. The string that we want to find.
# 
# The mapper function receives a chunk of filenames and calculates all occurences of the target string on them. If a file contains no occurences, we chose to not include an entry for that file in the dictionary result.
# 
# The reducer function uses the dict.update() method to merge the result dictionaries. MapReduce is used to create a function that, given a string, creates a dictionary where the keys are the file names and the values are lists with all line indexes that contain the given string.
# 
# Notice that the target variable will be defined outside and will be the string that we are looking for.

# In[71]:


# The target variable is defined outside and contains the string 
def map_grep(file_names):
    results = {}
    for fn in file_names:
        with open(fn) as f:
            lines = [line for line in f.readlines()]
        for line_index, line in enumerate(lines):
            if target in line:
                if fn not in results:
                    results[fn] = []
                results[fn].append(line_index)
    return results

def reduce_grep(lines1, lines2):
    lines1.update(lines2)
    return lines1

def mapreduce_grep(path, num_processes):
    file_names = [os.path.join(path, fn) for fn in os.listdir(path)]
    return map_reduce(file_names, num_processes,  map_grep, reduce_grep)


# ## Finding the occurences of "data"
# Using the function created using MapReduce, find all occurrences of the string "data" in the files stored in the wiki folder.

# In[72]:


target = "data"
data_occurrences = mapreduce_grep("wiki", 8)


# ## Allow for case insensitive matches
# We can allow for case insensitive matches by converting both the target and the file contents to lowercase before they are matched.  

# In[73]:


def map_grep_insensitive(file_names):
    results = {}
    for fn in file_names:
        with open(fn) as f:
            lines = [line.lower() for line in f.readlines()]
        for line_index, line in enumerate(lines):
            if target.lower() in line:
                if fn not in results:
                    results[fn] = []
                results[fn].append(line_index)
    return results

def mapreduce_grep_insensitive(path, num_processes):
    file_names = [os.path.join(path, fn) for fn in os.listdir(path)]
    return map_reduce(file_names, num_processes,  map_grep_insensitive, reduce_grep)

target = "data"
new_data_occurrences = mapreduce_grep_insensitive("wiki", 8)


# ## Checking for additional matches
# We already stored the results into variables data_occurences and new_data_occurrences. To check for additional matches with the second version of the algorithm, we can loop over the file names and print the length difference between the results. 
# 
# Let's verify that the new implementation works by seeing if it finds more matches than the previous implementation.

# In[74]:


for fn in new_data_occurrences:
    if fn not in data_occurrences:
        print("Found {} new matches on file {}".format(len(new_data_occurrences[fn]), fn))
    elif len(new_data_occurrences[fn]) > len(data_occurrences[fn]):
        print("Found {} new matches on file {}".format(len(new_data_occurrences[fn]) - len(data_occurrences[fn]), fn))


# ## Finding match indexes on lines
# Given a string and a target, find all occurrences of the target within the string.

# In[75]:


def find_match_indexes(line, target):
    results = []
    i = line.find(target, 0)
    while i != -1:
        results.append(i)
        i = line.find(target, i + 1)
    return results

# Test implementation
s = "Data science is related to data mining, machine learning and big data.".lower()
print(find_match_indexes(s, "data"))


# ## Finding all match locations
# We can use any of the above functions to find all match locations. We will use the third function. 
# 
# After finding all indexes in one line, we need to create pairs by adding the line index.

# In[76]:


def map_grep_match_indexes(file_names):
    results = {}
    for fn in file_names:
        with open(fn) as f:
            lines = [line.lower() for line in f.readlines()]
        for line_index, line in enumerate(lines):
            match_indexes = find_match_indexes(line, target.lower())
            if fn not in results:
                results[fn] = []
            results[fn] += [(line_index, match_index) for match_index in match_indexes]
    return results

def mapreduce_grep_match_indexes(path, num_processes):
    file_names = [os.path.join(path, fn) for fn in os.listdir(path)]
    return map_reduce(file_names, num_processes,  map_grep_match_indexes, reduce_grep)

target = "science"
occurrences = mapreduce_grep_match_indexes("wiki", 8)


# ## Displaying the results
# Our grep algorithms can now find all the matches, however, with the dictionary it produces, it's not very easy to see those matches.
# 
# We will write the results into a CSV file to more easily see those matches. We will create a CSV file listing all occurrences, and will also show the text around each occurrence.

# In[77]:


import csv

# How many character to show before and after the match
context_delta = 30

with open("results.csv", "w") as f:
    writer = csv.writer(f)
    rows = [["File", "Line", "Index", "Context"]]
    for fn in occurrences:
        with open(fn) as f:
            lines = [line.strip() for line in f.readlines()]
        for line, index in occurrences[fn]:
            start = max(index - context_delta, 0)
            end   = index + len(target) + context_delta
            rows.append([fn, line, index, lines[line][start:end]])
    writer.writerows(rows)


# Here's an example of the table created by our solution. The target was the string "science:"

# In[78]:


import pandas
df = pandas.read_csv("results.csv")
df.head(10)


# ## Conclusion
# Locating data from text files is a very common and time-consuming operation when many files are involved. By using MapReduce, we can significantly reduce the time required to locate that data.
# 
# In this project, we've implemented a MapReduce grep algorithm that locates all matches of a given string within all files in a given folder.
