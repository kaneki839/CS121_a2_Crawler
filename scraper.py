import re
from urllib.parse import urlparse, urldefrag, urljoin, parse_qs
from bs4 import BeautifulSoup
from collections import defaultdict
from urllib import robotparser

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

visited_unique_pages = {} # key: hash of the content, val: url itself
max_tokens = 0  # words that longest page contains
longest_page_url = "" 
word_freqs = defaultdict(int)  # frequencies of all words
links_in_domain = defaultdict(set)  # all the links in the ics.uci.edu domain

robots_list = {}  # store robots rules that have been read; prevent reading robots.txt repeatly


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
    if url_content_hash not in visited_unique_pages:
        visited_unique_pages[url_content_hash] = url
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

    # Handle redirections
    if 300 <= resp.status < 400:
        return list(unique_links)
        
    if resp.status == 200 and resp.raw_response and resp.raw_response.content not in [None, ""]:  # check the status code is ok and the content is not empty

        if not is_allowed_by_robots(url):
            return list(unique_links)

        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        filtered_tokens = tokenize(soup.getText()) # tokenize the page
        if len(filtered_tokens) > 200:  # crawl pages with high textual information content: must more than 200 words
            if in_ics_domain(url):  # check if the url is in ics domain
                links_in_domain[urlparse(url).hostname].add(url)  # how many links in the domain
            
            if len(resp.raw_response.content) > 500000000: # avoiding crawing too large files : 500 MB limits
                return list(unique_links)
            
            if url in visited_unique_pages.values():  # Handle infinite traps
                return list(unique_links)
            
            if not is_similar_page(url, filtered_tokens): # check if the crawling the similar page with no information
                for token in filtered_tokens:  # update word freqencies
                    word_freqs[token] += 1

                if len(filtered_tokens) > max_tokens: # find the longest page
                    max_tokens = len(filtered_tokens)
                    longest_page_url = url
                
                links = soup.find_all('a')  # get all the url tag in the page
                for link in links:
                    obtained_link = link.get('href')   # get the url inside the tag
                    if obtained_link:  # check it's not empty url
                        abs_url = urljoin(url, urldefrag(obtained_link).url) # compose absolute url by joinging base url and defrag. url
                        unique_links.add(abs_url)   # add the absolute the url
                print(f"URL crawled => {url}")
                print(links_in_domain)
    else:
        print(f"ERROR when crawling {url}: HTTP Status {resp.status} - {resp.error}")
    return list(unique_links)

def in_ics_domain(url):
    subdomain = urlparse(url).hostname
    return subdomain and subdomain.endswith(".ics.uci.edu") and subdomain != "www.ics.uci.edu"


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    print(url)
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        print("pass scheme")

        # Check whether the URL is within the domains
        if not is_within_domain(parsed):
            return False
        print("pass domain")

        # Filter increment numbers in the path: e.g /page100 | /1 | /a
        if re.search(r'(/page\d+)|(/\d+)|(/[a-z]$)', parsed.path):
            return False
        print("pass path")

        # Filter urls that include date (lots of urls with date are "no real data")。
        if not contains_date_pattern:
            return False
        print("pass date")

        # Filter urls with more than 15 query parameters
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        if len(query_params) > 5:
            return False
        print("pass query")

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4|mpg"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz"
            + r"|json|xml|sql|yaml|ini|flv|3gp|aab|apk|webp|heic|bat|cmd|sh|txt)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise


# Check whether the URL is within the domains
def is_within_domain(parsed_url):
    allowed_domains = [
        r'.*\.ics\.uci\.edu',
        r'.*\.cs\.uci\.edu',
        r'.*\.informatics\.uci\.edu',
        r'.*\.stat\.uci\.edu'
    ]

    netloc = parsed_url.netloc
    for domain in allowed_domains:
        if re.match(domain, netloc):
            return True
    return False


def contains_date_pattern(parsed_url):
    # Matches common date formats
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
        r'\d{4}\d{2}\d{2}',  # YYYYMMDD
        r'/\d{2}-\d{2}-\d{4}/',  # DD-MM-YYYY
        r'/\d{2}/\d{2}/\d{4}/'  # DD/MM/YYYY
    ]
    # Check whether a date pattern is included
    for pattern in date_patterns:
        if re.search(pattern, parsed_url.path):
            return True
    return False


# Check if the url is disallowed to crawl.
def is_allowed_by_robots(url, user_agent="*"):
    rp = robotparser.RobotFileParser()
    parsed_url = urlparse(url)
    scheme = parsed_url.scheme
    netloc = parsed_url.netloc

    if netloc in robots_list:
        return robots_list[netloc].can_fetch(user_agent, url)

    robot_url = f"{scheme}://{netloc}/robots.txt"
    rp.set_url(robot_url)
    try:
        print("try read robot")
        rp.read()  # Get the rules from robots.txt
        print("read successfully")
        robots_list[netloc] = rp

        print("---CAN FETCH---")

        return rp.can_fetch(user_agent, url)
    except:
        return False


def report():
    sorted_word_freqs = sorted(word_freqs.items(), key = lambda item: item[1], reverse = True)
    with open(f"report.txt", 'w') as f:
        f.write("------------------------------R E P O R T------------------------------\n\n")
        f.write("Unique Pages Found: " + str(len(visited_unique_pages)) + "\n")
        f.write("\n")
        f.write(f"URL With the Largest Word Count: {longest_page_url} with {max_tokens} words \n")
        f.write("\n")
        f.write("50 Most Common Words:\n")
        for i in range(50):
            f.write(f"\t{i+1}. {sorted_word_freqs[i][0]} : {sorted_word_freqs[i][1]} \n")
        f.write("\n")
        f.write(f"Number of subdomains in the ics.uci.edu domain: {len(links_in_domain)} \n")
        f.write("Subdomains List: \n")
        index = 1
        for subdomain, pages in sorted(links_in_domain.items(), key=lambda x: x[0]):
            f.write(f"\t{index}. {subdomain}, {len(pages)}\n")
            index += 1