import os
import pytest

def split_file_every(x, snippets):
    count = 0
    number_of_files_made = 0
    for snippet in snippets:
        count += 1
        if count % x == 0:
            number_of_files_made +=1
    return number_of_files_made

def test_list_with_fewer_than_split_produces_one_file():
    assert split_file_every(1,["snip"]) == 1

def test_list_with_more_than_split_produces_two_files():
    assert split_file_every(1,["snip", "snip"]) == 2

def test_list_with_higher_split_produces_two_files():
    assert split_file_every(4,["snip", "snip","snip", "snip",
                                    "snip", "snip","snip", "snip"]) == 2
