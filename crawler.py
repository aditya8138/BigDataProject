import glob

read_files = []
for url in ('/Users/aditya/news-please-repo/data/2019/05/03/elcolombiano.com/*.json', '/Users/aditya/news-please-repo/data/2019/05/03/vanguardia.com/*.json', '/Users/aditya/news-please-repo/data/2019/05/03/larepublica.co/*.json', '/Users/aditya/news-please-repo/data/2019/05/03/elpais.com.co/*.json', '/Users/aditya/news-please-repo/data/2019/05/03/eluniversal.com.co/*.json'):
    read_files.extend(glob.glob(url))

with open("result.txt", "wb") as outfile:
    for f in read_files:
        with open(f, "rb") as infile:
            outfile.write(b",\n" + infile.read())