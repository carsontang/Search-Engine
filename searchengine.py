#! /usr/bin/python

# Search engine based on Toby Segaran's Programming Collective Intelligence
# Version: 0.1
# Author: Carson Tang <http://www.github.com/carsontang>

import re
import urllib2
from BeautifulSoup import *
from pysqlite2 import dbapi2 as sqlite
from urlparse import urljoin

class Crawler(object):
	"""
	A simple web crawler that can be seeded with a small set of pages.
	"""
	
	def __init__(self, dbname):
		"""
		Initialize the crawler with the name of database.
		"""
		self.stopwords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])
		self.con = sqlite.connect(dbname)
		
	def __del__(self):
		self.con.close()
		
	def dbcommit(self):
		self.con.commit()
	
	def _get_entry_id(self, table, field, value, createnew=True):
		"""
		Auxillary function for getting an entry id and adding
		it if it's not present.
		"""
		cur = self.con.execute("select rowid from %s where %s = '%s'" % (table, field, value))
		result = cur.fetchone()
		if result == None:
			cur = self.con.execute(
				"insert into %s (%s) values ('%s')" % (table, field, value))
			return cur.lastrowid
		else:
			return result[0]
	
	def add_to_index(self, url, soup):
		"""
		Index an individual page
		"""
		if self.is_indexed(url):
			return
		
		print 'Indexing ' + url
		
		# Get the individual words
		text = self.get_text_only(soup)
		words = self.separate_words(text)
		
		# Get the URL id
		url_id = self._get_entry_id('urllist', 'url', url)
		
		# Link each word to this url
		for word_loc in range(len(words)):
			word = words[word_loc]
			
			# Don't associate a word with a url if it's a stop word
			if word in self.stopwords:
				continue
			
			word_id = self._get_entry_id('wordlist', 'word', word)
			self.con.execute("insert into wordlocation(urlid, wordid, location) \
				values (%d, %d, %d)" % (url_id, word_id, word_loc))
		
	def get_text_only(self, soup):
		"""
		Extract the text from an HTML page (no tags).
		"""
		v = soup.string
		if v == None:
			c = soup.contents
			result_text = ''
			for text_node in c:
				subtext = self.get_text_only(text_node)
				result_text += subtext + '\n'
			return result_text
		else:
			return v.strip()
		
	def separate_words(self, text):
		"""
		Separate the words by any non-whitespace character.
		TO-DO: Make a better separator. Right now, strings like "zkjgowo2" and "zkouvqgg"
		are being accepted as "words".
		"""
		splitter = re.compile('\\W*')
		return [s.lower() for s in splitter.split(text) if s != '']
		
	def is_indexed(self, url):
		"""
		URL is indexed if the URL has a rowid in urllist.
		"""
		result = self.con.execute("select rowid from urllist where url='%s'" % url).fetchone()
		if result != None:
			# Check if URL has been crawled
			v = self.con.execute('select * from wordlocation where urlid = %d' % result[0]).fetchone()
			if v != None:
				return True
		return False
		
	def add_link_ref(self, url_from, url_to, link_text):
		"""
		Add a link between two pages.
		"""
		pass
		
	def crawl(self, pages, depth=2):
		"""
		Starting with a list of pages, do a breadth first search to the given
		depth, indexing pages as we go
		"""
		for i in range(depth):
			new_pages = set()
			for page in pages:
				try:
					response = urllib2.urlopen(page)
				except:
					print "Could not open %s" % page
					continue
				soup = BeautifulSoup(response.read())
				self.add_to_index(page, soup)
		
				# Get links from current page to add to crawler's repository
				# of links
				links = soup('a')
				for link in links:
					if ('href' in dict(link.attrs)):
						url = urljoin(page, link['href'])
						if url.find("'") != -1:
							continue
						url = url.split('#')[0] # remove location portion
						if url[0:4] == 'http' and not self.is_indexed(url):
							new_pages.add(url)
						link_text = self.get_text_only(link)
						self.add_link_ref(page, url, link_text)
				
				self.dbcommit()
			pages = new_pages
						
	def create_index_tables(self):
		self.con.execute('create table urllist(url)')
		self.con.execute('create table wordlist(word)')
		self.con.execute('create table wordlocation(urlid, wordid, location)')
		self.con.execute('create table link(fromid integer, toid integer)')
		self.con.execute('create table linkwords(wordid, linkid)')
		self.con.execute('create index wordidx on wordlist(word)')
		self.con.execute('create index urlidx on urllist(url)')
		self.con.execute('create index wordurlidx on wordlocation(wordid)')
		self.con.execute('create index urltoidx on link(toid)')
		self.con.execute('create index urlfromidx on link(fromid)')
		self.dbcommit()

class Searcher(object):
	def __init__(self, dbname):
		self.con = sqlite.connect(dbname)
	
	def __del__(self):
		self.con.close()
		
	def get_match_rows(self, query):
		"""
		Return tuple that includes (URL, query_0 location, query_1 location, ...)
		"""
		# Strings to build the query
		field_list = 'w0.urlid'
		table_list = ''
		clause_list = ''
		word_ids = []
		
		# Split the words by spaces
		words = query.split(' ')
		table_number = 0
		
		for word in words:
			# Get the word ID
			word_row = self.con.execute("select rowid from wordlist where word='%s'" % word).fetchone()
			if word_row != None:
				word_id = word_row[0]
				word_ids.append(word_id)
			
				# Modify the query appropriately if there is more than one table
				if table_number > 0:
					table_list += ','
					clause_list += ' and '
					clause_list += 'w%d.urlid=w%d.urlid and ' % (table_number-1, table_number)
				field_list += ',w%d.location' % table_number
				table_list += 'wordlocation w%d' % table_number
				clause_list += 'w%d.wordid=%d' % (table_number, word_id)
				table_number += 1
			else:
				print 'No rows for %s' % word
		
		# Look for a URL that contains all of the word's in words
		full_query = 'select %s from %s where %s' % (field_list, table_list, clause_list)
		cursor = self.con.execute(full_query)
		rows = [row for row in cursor]
		return rows, word_ids
		
	def get_scored_list(self, rows, word_ids):
		
		# row[0] is a URL
		# Create a mapping of URL to score
		total_scores = dict([(row[0], 0) for row in rows])
		
		# This is where I'll put the scoring functions
		weights = [(1.0, self._location_score(rows)), (1.5, self._frequency_score(rows))]
		for (weight, scores) in weights:
			for url in total_scores:
				total_scores[url] += weight * scores[url]
				
		return total_scores

	def get_url_name(self, id):
		return self.con.execute("select url from urllist where rowid=%d" % id).fetchone()[0]

	def _normalize_scores(self, scores, small_is_better=0):
		vsmall = 0.00001 # Avoid division by zero errors
		if small_is_better:
			min_score = min(scores.values())
			return dict([(url, float(min_score)/max(vsmall,score)) for (url, score) in scores.items()])
		else:
			max_score = max(scores.values())
			if max_score == 0:
				max_score = vsmall
			return dict([(url, float(score)/max_score) for (url, score) in scores.items()])
	
	# IMPLEMENT TF-IDF
	# understand how the normalizing score works
	def _frequency_score(self, rows):
		
		# Initialize (URL, frequency) mapping
		counts = dict([(row[0], 0) for row in rows])
		for row in rows:
			counts[row[0]] += 1
		return self._normalize_scores(counts)
		
	def _location_score(self, rows):
		"""
		The smaller the location score, the more likely the word
		is at the top of the page, the more likely it's a topic.
		"""
		locations = dict([(row[0], 1000000) for row in rows])
		for row in rows:
			# Sum the locations
			loc = sum(row[1:])
			if loc < locations[row[0]]:
				locations[row[0]] = loc
		
		return self._normalize_scores(locations, small_is_better=1)
		
	def query(self, q, top_n_results=10):
		rows, word_ids = self.get_match_rows(q)
		scores = self.get_scored_list(rows, word_ids)
		ranked_scores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
		for (score, url_id) in ranked_scores[0:top_n_results]:
			print '%f\t%s' % (score, self.get_url_name(url_id))
			
	def feeling_lucky(self, q):
		"""
		Return the top result, a la Google.
		"""
		self.query(q, 1)
		