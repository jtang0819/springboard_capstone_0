import raw_data_clean
import clean_data_to_CleanDataDB



if __name__ == '__main__':
    print("Begin Run")
    clean = raw_data_clean.consume()
    print("End Run")

# look into multiprocessing
# how to end the script ie. timeout, sys.exit