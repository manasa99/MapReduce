import re
from src.utils.ApplicationInterface import ApplicationInterface as App
from collections import defaultdict


class Application(App):

    def __init__(self, lines=None, file_ptr: int = 0, mapped_data=[]):
        self.lines = lines
        self.file_ptr = file_ptr
        self.mapped_data = mapped_data
        self.map_dict = defaultdict(int)
        self.final_result = None

    def map_word_count(self, line):
        words = re.findall(r'\w+', line.lower())
        for word in words:
            self.map_dict[word] += 1

    def map_data(self):
        if self.mapped_data:
            print("map data exists")
        else:
            self.mapped_data = []

        for line in self.lines:
            self.map_word_count(line)
        for word in self.map_dict:
            self.mapped_data.append(word + " " + str(self.map_dict[word]))
        return self.mapped_data

    def reduce_data(self):
        return self.mapped_data[0][0] + " " + str(sum([int(i[1]) for i in self.mapped_data]))


# if __name__ == '__main__':
#     lines = ["the quick brown fox jumps over the lazy dog",
#              "now is the time for all good men to come to the aid of their country",
#              "ask not what your country can do for you ask what you can do for your country"
#              "the quick brown fox jumps over the lazy dog",
#              "now is the time for all good men to come to the aid of their country",
#              "ask not what your country can do for you ask what you can do for your country"
#              "the quick brown fox jumps over the lazy dog", ]
#     map_wordcount = Application(lines=lines)
#     mapped_data_ = map_wordcount.map_data()
#     mapped_data_ = [mapped_data_[0]] * 10
#     mapped_data_ = [(i.split()[0], (i.split()[1])) for i in mapped_data_]
#     print(mapped_data_)
#     reduce_wordcount = Application(mapped_data=mapped_data_)
#     reduced_data_ = reduce_wordcount.reduce_data()
#     print(reduced_data_)
