import raw_data_clean
import clean_data_to_CleanDataDB


if __name__ == '__main__':
    clean = raw_data_clean.consume()
    print(clean)

