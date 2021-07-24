import raw_data_clean
import clean_data_to_CleanDataDB

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    clean = raw_data_clean.consume()
    print(clean)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
