# -*- coding: utf-8 -*-

from rake_nltk import Rake


def get_top_n_keywords_from_text(text, n):
    r = Rake()
    r.extract_keywords_from_text(text)
    lst = list(r.get_ranked_phrases_with_scores())

    return lst[:n]

if __name__ == '__main__':
    text = 'LE 16.4ft LED Flexible Light Strip, 300 Units SMD 2835 LEDs, 12V DC Non-waterproof, Light Strips, LED ribbon, DIY Christmas Holiday Home Kitchen Car Bar Indoor Party Decoration (Daylight White) '
    n = 11
    for a,b in get_top_n_keywords_from_text(text, n):
        print(a,b)