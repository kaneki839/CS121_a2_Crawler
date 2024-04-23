import re
from urllib.parse import urlparse, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict

stop_words = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't",
              'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by',
              "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't",
              'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have',
              "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him',
              'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't",
              'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor',
              'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out',
              'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some',
              'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there',
              "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to',
              'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were',
              "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's",
              'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're",
              "you've", 'your', 'yours', 'yourself', 'yourselves']

total_unique_pages = {}
max_tokens = 0  # words that longest page contains
longest_page_url = "" 
word_freqs = defaultdict(int)  # frequencies of all words
links_in_domain = defaultdict(set)  # all the links in the ics.uci.edu domain

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def tokenize(html_text):
    eng_tokens = re.findall(r'\b[a-zA-Z][a-zA-Z\']*[a-zA-Z]\b', html_text)  # tokenize 
    filtered_tokens = []
    for token in eng_tokens:
        if token and token.lower() not in stop_words:  # filter out stop words and empty string
            filtered_tokens.append(token.lower())
    return filtered_tokens

def is_similar_page(url, contents):
    '''
    contents: list of tokens
    '''
    url_content_hash = hash(tuple(contents))  # hash the content
    if url_content_hash not in total_unique_pages:
        total_unique_pages[url_content_hash] = url
        return False
    return True

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    global stop_words
    global max_tokens, longest_page_url
    
    unique_links = set()  # list of unique links
    if resp.status == 200 and resp.raw_response.content not in [None, ""]:  # check the status code is ok and the content is not empty
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        filtered_tokens = tokenize(soup.getText()) # tokenize the page
        if len(filtered_tokens) > 300:  # crawl pages with high textual information content: must more than 300 words
            if in_ics_domain(url):  # check if the url is in ics domain
                links_in_domain[urlparse(url).hostname].add(url)  # how many links in the domain

            if len(resp.raw_response.content) > 100000000: # avoiding crawing too large files : 100 MB limits
                return list(unique_links)
            # TODO: check similarity, redirect, and infinite traps
            if not is_similar_page(url, filtered_tokens): # check if the crawling the similar page with no information
                for token in filtered_tokens:  # update word freqencies
                    word_freqs[token] += 1

                if len(filtered_tokens) > max_tokens: # find the longest page
                    max_tokens = len(filtered_tokens)
                    longest_page_url = url
                
                links = soup.find_all('a')  # get all the url tag in the page
                for link in links:
                    if link.get('href'):  # check it's not empty url
                        obtained_link = link.get('href')  # get the url inside the tag
                        unique_links.add(urldefrag(obtained_link).url) # add the defragmented the url
                print(f"URL crawled => {url}")
                print(links_in_domain)
    else:
        print(f"ERROR when Crawling {url}: {resp.error}")
    return list(unique_links)

def in_ics_domain(url):
    subdomain = urlparse(url)
    return subdomain and subdomain.hostname.endswith(".ics.uci.edu") and subdomain != "www.ics.uci.edu"

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
