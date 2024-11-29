'''
Our goal is to get a youtube channel id and a youtube channel handle from a youtube channel url.

'''

import re
from urllib.parse import urlparse, parse_qs
from typing import Optional, Tuple

def extract_channel_info(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract YouTube channel ID and handle from a YouTube channel URL.
    
    Args:
        url (str): YouTube channel URL
        
    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing (channel_id, channel_handle)
        Either value could be None if not found
        
    Examples:
        >>> extract_channel_info("https://www.youtube.com/channel/UC_x5XG1OV2P6uZZ5FSM9Ttw")
        ('UC_x5XG1OV2P6uZZ5FSM9Ttw', None)
        
        >>> extract_channel_info("https://www.youtube.com/@Google")
        (None, '@Google')
    """
    # Initialize return values
    channel_id = None
    channel_handle = None
    
    try:
        # Parse the URL
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        
        # Handle different URL patterns
        if path.startswith('channel/'):
            # Format: youtube.com/channel/UC...
            channel_id = path.split('channel/')[1]
            
        elif path.startswith('@'):
            # Format: youtube.com/@handle
            channel_handle = path
            
        elif path.startswith('c/'):
            # Format: youtube.com/c/customname
            # Note: Custom URLs don't provide channel ID directly
            pass
            
        elif path.startswith('user/'):
            # Format: youtube.com/user/username
            # Note: Legacy format, doesn't provide channel ID directly
            pass
            
        # Additional validation
        if channel_id and not re.match(r'^UC[\w-]{22}$', channel_id):
            channel_id = None
            
        if channel_handle and not re.match(r'^@[\w-]+$', channel_handle):
            channel_handle = None
            
    except Exception as e:
        print(f"Error processing URL: {e}")
        return None, None
        
    return channel_id, channel_handle

# Example usage
def test_extractor():
    test_urls = [
        "https://www.youtube.com/channel/UCeF5sxjXSdWq80n3RA9gBpw",
        "https://www.youtube.com/channel/UCx9aAD3hFkwMF3zEnB-l8Uw",
        "https://www.youtube.com/channel/UCUwsH4xqHEa7M1CHMpcTwSg",
        "https://www.youtube.com/channel/UC-JOwSr-NE1XZHhk54MMmAw",
        "https://www.youtube.com/channel/UCTbmfnI9fElQ3g9L06QW0PQ",
        "https://www.youtube.com/channel/UCKKM5HNf3vWcgho3eof5biA",
        "https://www.youtube.com/channel/UCCQ96qg2uIdgKs2PSxKJkCQ", 
        "https://www.youtube.com/channel/UC5Zp9Tko1Jg-9uYlj6kBQwg",
        "https://www.youtube.com/channel/UCuaQI0EMK-m_CFG3cquqw9Q",
        "https://www.youtube.com/@bangtkenter",
        "https://www.youtube.com/channel/UCaYiBRR7T4wr2nSa5yM3iCA",
        "https://www.youtube.com/channel/UCks3zr_CgdnZ_OG7QvKDvvA",
        "https://www.youtube.com/channel/UCXjTiSGclQLVVU83GVrRM4w",
        "https://www.youtube.com/@baS0NLr2Hjkf8p9SGS_-Aw",
        "https://www.youtube.com/channel/UClFUlwfo_0bXs2pRtUvS00w",
        "https://www.youtube.com/channel/UCXl7QZ4pMoPp8GnivahN8Jw",
        "https://www.youtube.com/channel/UCFjnlvnRWYVUs1ztXEVl9oQ",
        "https://www.youtube.com/@losangelesopera",
        "https://www.youtube.com/channel/UC4IPy7-vQCG2WAHn1qjZYvA",
        "https://www.youtube.com/channel/UCLAEihC5RkubOttRnUjB0Fw",
        "https://www.youtube.com/@canal12sv",
        "https://www.youtube.com/channel/UC4EAhpJhAhHrnI5TPY89kdA",
        "https://www.youtube.com/channel/UCgDk7WxfkdMlYlGElMTFZyQ",
        "https://www.youtube.com/channel/UCp-S2rmd7Ta-MD3tbU8AVAg",
        "https://www.youtube.com/channel/UCw_NyJK7EzwZwJv49NG7fYA",
        "https://www.youtube.com/channel/UCR8KEjrknc85um3LqpKDSRw",
        "https://www.youtube.com/channel/UCqOszJxvqeFsMoU8lYFMS0w",
        "https://www.youtube.com/channel/UCKxeyGZg-7RZ8SqQ-y_QJAA",
        "https://www.youtube.com/channel/UCaEd_c_vrhiWq2KtZj8XuXg",
        "https://www.youtube.com/channel/UC1O0jDlG51N3jGf6_9t-9mw",
        "https://www.youtube.com/channel/UCxp-ulKLd5lXSvMJrx7Yl5g",
        "https://www.youtube.com/channel/UC2o1_8Vj_GicEGYA1eriHsQ",
        "https://www.youtube.com/channel/UCjS8nl2TMe5RIZqy4SS-I6w",
        "https://www.youtube.com/channel/UCmxsHgVl-5qH-tZia4O9woQ",
        "https://www.youtube.com/@blogilates",
        "https://www.youtube.com/@gstvnewschannel",
        "https://www.youtube.com/@vegetta777",
        "https://www.youtube.com/channel/UCFnpyYEILQRtBSl4qEd8Yow",
        "https://www.youtube.com/channel/UCkhBnxfBU4r47c6CtDNWIfg",
        "https://www.youtube.com/channel/UCSNkfKl4cU-55Nm-ovsvOHQ",
        "https://www.youtube.com/@tmntmovie1",
        "https://www.youtube.com/channel/UC2Q3W0H8ZSGQ5c6CU_iXxBg",
        "https://www.youtube.com/channel/UCpgbRy8qadkEt5y3heggSnw",
        "https://www.youtube.com/@dcw97wzaNJDaY90f9PwNgA",
        "https://www.youtube.com/channel/UCx5Y0M6LA98VYkC--lIq_zg",
        "https://www.youtube.com/channel/UCEtKiUq51d4aUN9ieE79MaQ",
        "https://www.youtube.com/channel/UCzAx4uapdsvrybgBucDmkQQ",
        "https://www.youtube.com/channel/UCTujOpyqqNY53BZmuijBVBw",
        "https://www.youtube.com/channel/UCj7tpPKHmV4-JN15MzoJCeA",
        "https://www.youtube.com/@M8tZcnnJOFZr5sPWRiscUg",
        "https://www.youtube.com/channel/UCJdeeHsPtp0PQF9kkgM4bkA",
        "https://www.youtube.com/channel/UCT7a_fVlSrjOs9jyvtH-uhA",
        "https://www.youtube.com/@nar907",
        "https://www.youtube.com/channel/UCI_gosTkoxO4XEe54nKr7cA",
        "https://www.youtube.com/channel/UCzlZVuFNhjUT4K4Q-MErCIA",
        "https://www.youtube.com/channel/UClUVQJQL-yGKYiceiJpNg2Q",
        "https://www.youtube.com/@3BIrgAatXukMjkhn7027mw",
        "https://www.youtube.com/channel/UCqX8KVDc0pmwvXgXJ2_WPvw",
        "https://www.youtube.com/@sh0-u4d_EpPQWvp6FkHonQ",
        "https://www.youtube.com/@googleindia",
        "https://www.youtube.com/channel/UC2TjHxVHneMazfJl_VInCaw",
        "https://www.youtube.com/channel/UCbG-IYEesqzV-vxZILoKY2w",
        "https://www.youtube.com/channel/UCtiRitTi4CbVF8DQ3TTnImQ",
        "https://www.youtube.com/channel/UCcsOhBOv3H7t28_1ah3DDCg",
        "https://www.youtube.com/channel/UCEg4_mU2CqqtQxWwzL_uh-w",
        "https://www.youtube.com/@zE8J287ECpkeV0VekLQjjw",
        "https://www.youtube.com/@theeducvideos",
        "https://www.youtube.com/channel/UCNJ-m0M85ESwKdvpZiJFOjQ",
        "https://www.youtube.com/channel/UCh6VzpC3iyZ_vjZ0QcKFKww",
        "https://www.youtube.com/channel/UCCwzr6YQuMeCkEnBGvqWtZg",
        "https://www.youtube.com/channel/UCvenXNE_H7qOm9InFB82vYg",
        "https://www.youtube.com/channel/UCMitywRgbEyuI1-3UWLv2lw",
        "https://www.youtube.com/channel/UCJEV2-yRw793YuRpwzTmDlQ",
        "https://www.youtube.com/@IndiaAstrologer",
        "https://www.youtube.com/channel/UCPjorGcmyAxDjmZ8x9nyg6A",
        "https://www.youtube.com/channel/UChrcgyrbk4RZ98CsMiEbyKA",
        "https://www.youtube.com/channel/UCLoy1IubCcBoZ4sLbKTIUig",
        "https://www.youtube.com/@paypal",
        "https://www.youtube.com/channel/UClHbjVXWB1rCNfB-MQDw-Nw",
        "https://www.youtube.com/@rejithrajtr",
        "https://www.youtube.com/channel/UCTLVzbdzqe_iGgb8pQQR-ew",
        "https://www.youtube.com/channel/UCyDCghdNx_lOMmKgwGq_hhA",
        "https://www.youtube.com/channel/UCLAB_dxPBpJHT2j8jgcQI9Q",
        "https://www.youtube.com/channel/UCBhyCzdAQyyBgvdPiGclnyw",
        "https://www.youtube.com/channel/UCFh1OK9za2KBEfi8sOv9fnQ",
        "https://www.youtube.com/channel/UCNtwAL7w6mvZ6XXoYseVm-A",
        "https://www.youtube.com/channel/UC_ZGVruqQ2fQUNnBqbNZYTg",
        "https://www.youtube.com/@vEyucb-7MaSM9gGLvPslqA",
        "https://www.youtube.com/channel/UC1GtfEVCV2tHwWNT_wU14Lg",
        "https://www.youtube.com/@70M2f_XaODPefyDa_xVN8Q",
        "https://www.youtube.com/channel/UCfCDDsdBxIwCxOkfCXbExLg",
        "https://www.youtube.com/channel/UCA1XN2IiFvGbefVh3AYX34Q",
        "https://www.youtube.com/channel/UCxtsuc6sy0Xx2ps0ZZJPXNw",
        "https://www.youtube.com/@crunchyrollpromo",
        "https://www.youtube.com/channel/UCyKzv7tJFROxJYz1iS76gwA",
        "https://www.youtube.com/channel/UCAodIC3rcPHrbLMYkk9NVzw",
        "https://www.youtube.com/channel/UCJpVp78ev9wFMHVJYwkrqvg",
        "https://www.youtube.com/channel/UCzHGNHKPptsSvRuicXkdMpQ",
        "https://www.youtube.com/@catapultfilms",
        "https://www.youtube.com/channel/UCC3knWAoyl1pxmfHUjBniXA"
    ]
    
    for url in test_urls:
        channel_id, handle = extract_channel_info(url)
        print(f"\nURL: {url}")
        print(f"Channel ID: {channel_id}")
        print(f"Handle: {handle}")

if __name__ == "__main__":
    test_extractor()