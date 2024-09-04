from dlt.sources.helpers import requests
import dlt

@dlt.resource(write_disposition = "append")
def hacker_news(
        orchestration_tools: list[str]):
    """
    This function fetches stories related to specified orchestration tools from Hackernews.
    For each tool, it retrieves the top 5 stories that have at least one comment.
    The stories are then appended to the existing data.

    Args:
        orchestration_tools: A tuple containing the names of orchestration tools for which stories are to be fetched.

    Yields:
        A generator that yields dictionaries. Each dictionary represents a story and contains the tool name along with the story details returned by the API request.
    """

    for tool in orchestration_tools:
        response = requests.get(f'http://hn.algolia.com/api/v1/search?query={tool}&tags=story&numericFilters=num_comments>=1&hitsPerPage=5')
        data = response.json()
        # Add the tool name to each story
        data["hits"] = [{"tool_name": tool, **item} for item in data["hits"]]
        # Yield each story one by one
        yield from data["hits"]


@dlt.transformer(data_from = hacker_news, write_disposition = "append")
def comments(story):
    """
    This function fetches comments for each story yielded by the 'hacker_news' function.
    It calculates the number of pages of comments based on the number of comments each story has,
    and fetches comments page by page. The comments are then appended to the existing data.

    Args:
        story: A dictionary representing a story, yielded by the 'hacker_news' function.

    Yields:
        A generator that yields lists of dictionaries. Each list represents a page of comments,
        and each dictionary within the list represents a comment and contains the tool name, story title,
        story URL, sentiment of the comment, and the comment details returned by the API request.
    """

    tool_name = story["tool_name"]
    story_title = story["title"]
    story_id = story["story_id"]
    url = story.get("url")
    num_comments = story["num_comments"]

    num_pages = int(num_comments/20) # The API returns 20 comments per page
    if num_pages != num_comments/20:
        num_pages += 1

    for page in range(num_pages):
        response = requests.get(f'http://hn.algolia.com/api/v1/search?tags=comment,story_{story_id}&page={page}')
        data = response.json()
        # Add the tool name, story title, story URL, and sentiment to each comment
        data["hits"] = [{"tool_name": tool_name, "story_title": story_title, "story_url": url, "sentiment": openai_sentiment(item["comment_text"]), **item} for item in data["hits"]]
        #data["hits"] = [{"tool_name": tool_name, "story_title": story_title, "story_url": url, **item} for item in data["hits"]] # Without sentiment_analysis
        # Yield each page of comments
        yield data["hits"]


@dlt.source()
def hacker_news_full(orchestration_tools:tuple[str] = ("Airflow", )):
    """
    This function is a dlt source that groups together the resources and transformers needed to fetch
    Hackernews stories and their comments for specified orchestration tools.

    Args:
        orchestration_tools: A tuple containing the names of orchestration tools for which Hacker News stories and comments are to be fetched.

    Yields:
        A generator that yields the results of the 'hacker_news' resource piped into the 'comments' transformer.
    """

    # The 'hacker_news' resource fetches stories for the specified orchestration tools
    # The 'comments' transformer fetches comments for each story yielded by the 'hacker_news' resource
    yield hacker_news(orchestration_tools = orchestration_tools) | comments