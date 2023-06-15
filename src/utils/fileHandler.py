import os


class FileHandler:

    def __init__(self, file_path: str, lines: list = None):
        self.file_path = file_path
        self.lines = lines

    def read_file(self):
        try:
            with open(self.file_path) as file:
                self.lines = [line.rstrip() for line in file]
            return self.lines
        except Exception as e:
            print(e)
        except OSError:
            self.lines = []
            return self.lines

    def write_file(self):
        if not self.lines:
            print("Nothing to write")
        try:
            with open(self.file_path, 'w+') as f:
                f.writelines("\n".join(self.lines))
            return True
        except Exception as e:
            print(e)
        except OSError:
            print("Error with file")
        return False


def chunk_file_by_lines(file_path, n_parts, output_dir):
    def next_line(open_file):
        return open_file.readline()
    output_files = []
    os.makedirs(output_dir, exist_ok=True)
    with open(file_path, 'r') as f:
        total_lines = sum(1 for line in f)
    orig = open(file_path, 'r')
    line_ptr = 1
    for i in range(n_parts-1):
        chunk_path = os.path.join(output_dir, f'chunk{i}')
        lines_written = 0
        with open(chunk_path, 'w') as f:
            while lines_written < total_lines//n_parts:
                f.write(next_line(orig))
                lines_written += 1
        output_files.append((chunk_path, line_ptr))
        line_ptr += lines_written
    chunk_path = os.path.join(output_dir, f'chunk{n_parts-1}')
    lines_written = line_ptr-1
    with open(chunk_path, 'w') as f:
        while lines_written <= total_lines:
            f.write(next_line(orig))
            lines_written += 1
    output_files.append((chunk_path, line_ptr))
    orig.close()
    return output_files
