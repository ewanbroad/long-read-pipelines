import urllib.request


def fetch_timing_html(cromwell_server: str, submission_id: str, local_html: str) -> None:
    """
    For fetching timing chart of a cromwell execution and saving that to a local HTML page
    :param cromwell_server: cromwell server address
    :param submission_id: hex-string uuid of the submission
    :param local_html: where to save locally
    """
    s = cromwell_server.rstrip('/')
    timing_url = f'{s}/api/workflows/v1/{submission_id}/timing'
    urllib.request.urlretrieve(timing_url, local_html)
