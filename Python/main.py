import raw_data_clean
import clean_data_to_CleanDataDB
import social_data
import thread_data
import clean_data


if __name__ == '__main__':
    print("Begin Run")
    #clean = raw_data_clean.consume()
    social_data.consume()
    thread_data.consume()
    clean_data.consume()
    print("End Run")

# look into multiprocessing
