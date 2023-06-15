import re
from collections import defaultdict
from src.utils.ApplicationInterface import ApplicationInterface as App


class Application(App):

    def __init__(self, lines=None, file_ptr: int = 0, mapped_data=[]):
        self.lines = lines
        self.file_ptr = file_ptr
        self.mapped_data = mapped_data
        self.map_dict = defaultdict(list)
        self.final_result = None

    def map_inverted_index(self, line_num, line):
        words = re.findall(r'\w+', line.lower())
        for word_pos, word in enumerate(words):
            res = str(line_num)+":"+str(word_pos)
            self.map_dict[word].append(res)

    def map_data(self):
        if self.mapped_data:
            print("map data exists")
        else:
            self.mapped_data = []
        for curr_ptr, line in enumerate(self.lines):

            line_num = self.file_ptr + curr_ptr
            # print("line_num", line_num)
            self.map_inverted_index(line_num, line)

        for word in self.map_dict:
            value = word + " " + ",".join(self.map_dict[word])
            self.mapped_data.append(value)
            # str(self.map_dict[word]))
        return self.mapped_data

    def reduce_data(self):
        res = self.mapped_data[0][0] + " " + " ".join(" ".join(i[1].split(',')) for i in self.mapped_data)
        return res
        # self.mapped_data[0][0] + " " + str(sum([i[1] for i in self.mapped_data]))


# if __name__ == '__main__':
#     lines = ["the quick brown fox jumps over the lazy dog",
#              "now is the time for all good men to come to the aid of their country",
#              "ask not what your country can do for you ask what you can do for your country"
#              "the quick brown fox jumps over the lazy dog",
#              "now is the time for all good men to come to the aid of their country",
#              "ask not what your country can do for you ask what you can do for your country"
#              "the quick brown fox jumps over the lazy dog", ]
#     map_invertedindex = Application(lines=lines)
#     mapped_data_ = map_invertedindex.map_data()
#     print(mapped_data_)
#     mapped_data_ = [mapped_data_[0]] * 10
#     mapped_data_ = [(i.split()[0], (i.split()[1])) for i in mapped_data_]
#     print(mapped_data_)
#     reduce_wordcount = Application(mapped_data=mapped_data_)
#     reduced_data_ = reduce_wordcount.reduce_data()
#     print(reduced_data_)