# HMS-LogParser

/**
 * Accepts 2 command-line arguments : 1. Path to HMS log file, 2. Path to csv file where
 * parsed log information need to be written to. Parses the HMS log for PerfLogger entries,
 * Accepts 3 command-line arguments :
 *      1. Path to HMS log file (Required).
 *      2. Path to csv file where parsed log information need to be written to (Required).
 *      3. Boolean indicating whether to filter methods that do not create notifications (Optional, default=false).
 * Parses the HMS log for PerfLogger entries, and
 * obtains information about method name and timestamps from the perflogger entries,
 * Writes this information too a CSV file. Then, it filters the rows in CSV based on Strings
 * added in filterMethods to get only the relevant API calls.
 * Now, sorts the entries and finds the maximum concurrency noticed in the logs
 * based on the stasrt and end times of API calls for each method.
 * based on the start and end times of API calls for each method.
 */
