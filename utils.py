import re, html

def html2text(htm):
    # from https://stackoverflow.com/a/54296631/871738
    ret = ""
    try:
        ret = html.unescape(htm)
        ret = ret.translate({
            8209: ord('-'),
            8220: ord('"'),
            8221: ord('"'),
            160: ord(' '),
        })
        ret = re.sub(r"\s", " ", ret, flags = re.MULTILINE)
        ret = re.sub("<li>", "• ", ret, flags = re.IGNORECASE)
        ret = re.sub("<br>|<br />|</p>|</div>|</h\d>|</li>", "\n", ret, flags = re.IGNORECASE)
        ret = re.sub('<.*?>', ' ', ret, flags=re.DOTALL)
        ret = re.sub(r"  +", " ", ret)
    except:
        pass
    return ret