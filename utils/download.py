import requests
import cbor
import time

from utils.response import Response

def download(url, config, logger=None):
    host, port = config.cache_server
    resp = requests.get(
        f"http://{host}:{port}/",
        params=[("q", f"{url}"), ("u", f"{config.user_agent}")])

    # Handle redirects and index the url
    if 300 <= resp.status_code < 400:
        try:
            # allow redirects automatically, up to a maximum of 5.
            session = requests.Session()
            session.max_redirects = 5 # set redirect depth
            
            # would stop if non-redirect response is received, or exceeded the redirect limit
            new_resp = session.get(f"http://{host}:{port}/",
                                    params=[("q", f"{resp.url}"), ("u", f"{config.user_agent}")], 
                                    allow_redirects=True, )  
            resp = new_resp  # set the new valid response become the response function return
        except requests.TooManyRedirects:   # if exceeded the redirect limit, directly return with error msg
            logger.error(f"Exceeded the maximum number of allowed redirects: {resp} with url {url}.")
            return Response({
                "error": f"Exceeded the maximum number of allowed redirects: {resp} with url {url}.",
                "status": resp.status_code,
                "url": url})

    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        pass
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
