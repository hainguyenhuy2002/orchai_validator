class Logger:
    def __init__(self, file: str) -> None:
        self.__file = file

    def write(self, *msg, to_console=True):
        f = open(self.__file, mode='a', encoding='utf-8')
        f.write(" ".join([str(s) for s in msg]) + "\n")
        f.close()
        if to_console:
            print(*msg)