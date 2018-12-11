import sys
import subprocess
import time
import shutil
import os

import matplotlib.pyplot as plt

################ CONFIG VARIABLES ########

#workloads = ['citeseer', 'mico', 'patent', 'youtube', 'livej', 'orkut']

#### configurations contain tuple of [workloads], [queriesAnchored], [queriesUnanchored], [outliers_pcts], numServers, numPartitionsPerServer, outputActive

configurations = [
    (['citeseer'], [1,2,3,4,5,6,7,8], [1,2,3,4,5,6,7], ['0.0'], 1, 8, True),
    (['mico'], [1,2,3,4,5,6,7,8], [1,2,3,4,5,6,7], ['0.0'], 1, 8, True)
]

local = True 

fastNeighbors = 'true'
#outputBinary = 'true'
outputBinary = 'false'

#minMatches = '100'
minMatches = '0'

doRun = True
doVerify = True

################ DON'T TOUCH THESE VARIABLES ########

results = {'citeseer': [19, 14, 12, 0, 0, 0, 0, 0],
           'mico': [51980, 144511, 100925, 197303, 27638, 56987, 90815, 30862],
           'patent': [31343, 17320, 904287, 904789, 4570, 1098, 2552, 2208],
           'youtube': [1086315, 2726538, 11600303, 2897261, 352490, 437620, 531482, 221375]}

# !!!!NOTE !!!!: no counter name can be a superstring of another name

counters = ['processed', 'generated', 'output', 'matched', 'cross_edges', 'comput_time']

histograms = ['trees_expanded', 'embeddings_expanded', 'embeddings_expanded_pertree',
              'times_matched', 'trees_matched', 'embeddings_matched_pertree', 'max_embeddings_matched_pt',
              'edges_matched_pertree', 'max_edges_matched_pt', 'cost_matched_pertree', 'max_cost_matched_pt',
              'size_matched_pertree', 'max_size_matched_pt', 'trees_output']

timesPerTree = ['expanding', 'matching', 'output', 'sending', 'splitting', 'estimating', 'queueing']

arrays = ['time_cost', 'time_embeddings', 'time_edges']

series = ['metrics_pertree']

numServers = 0
numPartitionsPerServer = 0
outputActive = True


################ PREPARE EXPERIMENTS ################

def writeClusterYaml(numServers, numPartitionsPerServer, outputActive):
    clusterFile = open('cluster-spark.yaml', 'w')

    if local:
        clusterFile.write('spark_master: local[*]\n')
        clusterFile.write('worker_memory: 10g\n')
        clusterFile.write('driver_memory: 8g\n')
        clusterFile.write('num_workers: 1\n')
        clusterFile.write('num_compute_threads: 1\n')
    else:
        clusterFile.write('spark_master: yarn-client\n')
        clusterFile.write('worker_memory: 200g\n')
        clusterFile.write('driver_memory: 200g\n')
        clusterFile.write('num_workers: ' + str(numServers) + '\n')
        clusterFile.write('num_compute_threads: ' + str(numPartitionsPerServer) + '\n')

    clusterFile.write('max_result_size: 5g\n')
    clusterFile.close()


def writeAppYaml(workload, queryId, outliers_pct, minMatches, anchored, gj=''):
    yamlfile = open('search-configs.yaml', 'w')

    yamlfile.write('search_injective: true\n')
    # yamlfile.write('arabesque.graph.edge_labelled: false\n')
    # yamlfile.write('search_multigraph: false\n')
    yamlfile.write('search_outliers_pct: ' + outliers_pct + '\n')
    yamlfile.write('search_outliers_min_matches: ' + minMatches + '\n')
    yamlfile.write('search_fastNeighbors: ' + fastNeighbors + '\n')
    yamlfile.write('search_write_in_binary: ' + outputBinary + '\n')

    if anchored:
        yamlfile.write('search_query_graph_path: hdfs://localhost:8020/input/queries/Q' + str(queryId) + '-' + workload + '\n')
        yamlfile.write('search_output_path: output-search/' + gj + workload + '-' + str(queryId) + '\n')
        yamlfile.write('search_input_graph_path: ' + 'hdfs://localhost:8020/input/' + workload + '.unsafe.graph\n')
    else:
        yamlfile.write('search_query_graph_path: hdfs://localhost:8020/input/queries/Q' + str(queryId) + 'u\n')
        yamlfile.write('search_output_path: output-search/' + gj + str(queryId) + 'u\n')
        yamlfile.write('search_input_graph_path: ' + 'hdfs://localhost:8020/input/' + workload + '-single-label.graph\n')


    if outputActive:
        yamlfile.write('search_output_active: true\n')
    else:
        yamlfile.write('search_output_active: false\n')

    if workload == 'citeseer':
        yamlfile.write('search_num_vertices: 3312\n')
        yamlfile.write('search_num_edges: 9072\n')
        if anchored:
            yamlfile.write('search_num_labels: 6\n')
        else:
            yamlfile.write('search_num_labels: 1\n')

    if workload == 'mico':
        yamlfile.write('search_num_vertices: 100000\n')
        yamlfile.write('search_num_edges: 2160312\n')
        yamlfile.write('search_num_labels: 29\n')

    if workload == 'patent':
        yamlfile.write('search_num_vertices: 2745761\n')
        yamlfile.write('search_num_edges: 27930818\n')
        yamlfile.write('search_num_labels: 37\n')

    if workload == 'youtube':
        yamlfile.write('search_num_vertices: 4589876\n')
        yamlfile.write('search_num_edges: 87937596\n')
        yamlfile.write('search_num_labels: 108\n')

    if workload == 'star':
        yamlfile.write('search_num_vertices: 10000\n')
        yamlfile.write('search_num_edges: 39996\n')
        yamlfile.write('search_num_labels: 1\n')

    if workload == 'livej':
        yamlfile.write('search_num_vertices: 4846609\n')
        yamlfile.write('search_num_edges: 85702474\n')
        yamlfile.write('search_num_labels: 1\n')

    if workload == 'orkut':
        yamlfile.write('search_num_vertices: 3072441\n')
        yamlfile.write('search_num_edges: 234369798\n')
        yamlfile.write('search_num_labels: 1\n')

    yamlfile.write('log_level: fatal\n')
    yamlfile.write('system_type: search\n')

    yamlfile.close()

################ RUN EXPERIMENTS ################


def run(workload,queryId,outliers_pct,minMatches,anchored, gj=False):
    query = 'Q' + str(queryId)
    if not anchored:
        query += 'u'
    print ("\n############## running workload " + workload + " on query " + query + " with outlier_pct " + outliers_pct + " ##############\n")
    #subprocess.call(["hdfs", "dfs", "-rm", "output-search/*"])
    if gj:
        output_file = 'gj-'
        script_file = 'run_qfrag_spark_gj.sh'
    else:
        output_file = ''
        script_file = 'run_qfrag_spark.sh'
    writeAppYaml(workload,queryId,outliers_pct,minMatches,anchored, gj=output_file)
    print ('Arabesque is running workload ' + workload  + ' query ' + query + ' outliers_pct ' + outliers_pct + ' ...')
    p = subprocess.Popen(['./' + script_file + ' cluster-spark.yaml search-configs.yaml'], stderr=subprocess.PIPE, shell=True)
    #outChars = p.stderr.read()
    output, errors = p.communicate()
    errors = errors.decode("utf-8")
    #print(output)
    #print(errors)
    #print(outChars)
    # outLines = charsToLines(outChars, '')
    # for line in outLines:
    #     print(line)

    # outString = '\n'.join(outLines)
    # print(outString)
    idx_start = errors.find('@DEBUG Stats-Results:')
    idx_end = errors.find('@EndOfDEBUG')
    perf_numbers = errors[idx_start:idx_end]
    #print(perf_numbers)
    stats = perf_numbers.split('\n')
    stats_dict = {}
    for stat in stats:
        if '=' not in stat:
            continue
        stat = stat.replace('@DEBUG', '').strip()
        x = stat.split('=', 2)
        stats_dict[x[0].strip()] = x[1].strip()
        print(x[0])

    print(stats_dict['PureComputeTime'])
    return stats_dict['PureComputeTime']

def charsToLines(charsArray, startChar):
    lines = []
    currLine = ""
    copying = False
    for char in charsArray:
        if copying:
            if char == '\n':
                lines.append(currLine)
                currLine = ""
                copying = False
            else:
                currLine += str(char)
        elif (startChar == '' or char == startChar) and not copying:
            copying = True
            currLine += str(char)
    return lines

################ VERIFY RESULTS ################

def verifyGJ(workload, queryId, anchored=True):
    query = str(queryId)
    if not anchored:
        query += 'u'
        file_gj = 'output-search/gj-' + query
        file_gj_out = 'gj-' + query + '-0'
        file = 'output-search/' + query
        file_out = query + '-0'
    else:
        file_gj = 'output-search/gj-' + workload + '-' + query
        file_gj_out = './compare/gj-' + workload + '-' + query + '-0'
        file = 'output-search/' + workload + '-' + query
        file_out = './compare/' + workload + '-' + query + '-0'

    subprocess.call(['python', 'sort.py', file_gj, file_gj_out])
    subprocess.call(['python', 'sort.py', file, file_out])

    with open(file_gj_out) as f1:
        out1 = f1.readlines()
        set1 = set(out1)
    with open(file_out) as f2:
        out2 = f2.readlines()
        for line in out2:
            if line in set1:
                set1.remove(line)
            else:
                print("Mismatch")
                exit(1)
        if len(set1) != 0:
            print("Mismatch")
            exit(1)

def verifyAnchored(workload, queryId, anchored=True):
    query = str(queryId)
    if not anchored:
        file_out = 'sorted-sorted-clean-' + workload + '-unanchored-q' + query + '.txt'
        query += 'u'
        file_gj = 'output-search/gj-' + query + '/0'
        file_gj_out = 'compare/gj-' + query + '-0'
    else:
        file_gj = 'output-search/gj-' + workload + '-' + query + '/0'
        file_gj_out = 'compare/gj-' + workload + '-' + query + '-0'
        file_out = 'sorted-sorted-clean-' + workload + '-q' + query + '.txt'

    subprocess.call(['python', 'sort.py', file_gj, file_gj_out])

    with open('./' + file_gj_out) as f1:
        out1 = f1.readlines()
        set1 = set(out1)
    with open('./compare/' + file_out) as f2:
        out2 = f2.readlines()
        for line in out2:
            if line in set1:
                set1.remove(line)
            else:
                print("Mismatch")
                exit(1)
        if len(set1) != 0:
            print("Mismatch")
            exit(1)

# def verifyAnchored(workload, queryId, outliers_pct):
#     # NOTE for unanchored, it does not make sense to do this check as there could be different permutations
#     query = 'Q' + str(queryId)
#     searchOut = open(
#         'output-search/search-' + workload + '-' + query + '-outliers_pct=' + str(outliers_pct) + '-numServers=' + str(
#             numServers) \
#         + '-partPerServer=' + str(numPartitionsPerServer) + '-outputActive=' + str(outputActive) + '.txt', 'r')
#     numResults = sum(1 for line in searchOut)
#     searchOut.close()
#     expectedResults = results[workload][queryId - 1]
#     if numResults != expectedResults:
#         #        outcome = open ('failed-' + workload + '-' + query + '-' + outliers_pct + '.txt', 'w')
#         #        outcome.write('Problem with workload ' + workload + ' and query Q' + str(queryId))
#         #        outcome.write('Expected ' + str(expectedResults) + ' results and got ' + str(numResults))
#         #        outcome.close()
#         print ('Problem with workload ' + workload + ', query Q' + str(queryId) + ' and outliers_pct ' + str(outliers_pct))
#         print ('Expected ' + str(expectedResults) + ' results and got ' + str(numResults))
#         print ('Type Y to continue')
#         choice = raw_input()
#         if choice != 'Y':
#             sys.exit()
#         else:
#             return False
#     else:
#         print ('Workload ' + workload + ' and query Q' + str(queryId) + ' give right number of results')
#         return True


def verify(workload, queryId, outliers_pct, anchored):
    query = 'Q' + str(queryId)
    if not anchored:
        query += 'u'
    searchOut = open(
        'results/search-' + workload + '-' + query + '-outliers_pct=' + str(outliers_pct) + '-numServers=' + str(
            numServers) \
        + '-partPerServer=' + str(numPartitionsPerServer) + '-outputActive=' + str(outputActive) + '.txt', 'w')
    subprocess.call(["hdfs", "dfs", "-cat", "output-search/*"], stdout=searchOut)
    searchOut.close()

    searchOut = open(
        'results/search-' + workload + '-' + query + '-outliers_pct=' + str(outliers_pct) + '-numServers=' + str(
            numServers) \
        + '-partPerServer=' + str(numPartitionsPerServer) + '-outputActive=' + str(outputActive) + '.txt', 'r')
    search = set()
    for line in searchOut:
        words = line.split()
        words.sort(key=int)
        string = ""
        for word in words:
            string += word
            string += ' '
        search.add(string)
    searchOut.close()
    search = sorted(search)

    if anchored:
        truthOut = open('ground-truth/sorted-clean-' + workload + '-q' + str(queryId) + '.txt', 'r')
    else:
        truthOut = open('ground-truth/sorted-clean-' + workload + '-unanchored-q' + str(queryId) + '.txt', 'r')
    #    truth = set()
    #    for line in truthOut:
    #        words = line.split()
    #        words.sort(key=int)
    #        string = ""
    #        for word in words:
    #            string += word
    #            string += ' '
    #        truth.add(string)
    #    truthOut.close()
    #    truth = sorted(truth)

    for i in range(len(search)):
        truth = truthOut.readline()
        if truth == '':
            print ('Problem with workload ' + workload + ', query ' + query + ' and outliers_pct ' + str(outliers_pct))
            print ('Search produced a superset of the ground truth results')
            print ('Example of additional incorrect result ' + repr(search[i].strip(' \t\n\r')))
            print ('Type Y to continue')
            choice = raw_input()
            if choice == 'Y':
                return
            else:
                sys.exit()

        if search[i].strip(' \t\n\r') != truth.strip(' \t\n\r'):
            print ('Problem with workload ' + workload + ', query ' + query + ' and outliers_pct ' + str(outliers_pct))
            print ('Either search missed true result ' + repr(truth.strip(' \t\n\r')))
            print ('or it found false result ' + repr(search[i].strip(' \t\n\r')))
            print ('Type Y to continue')
            choice = raw_input()
            if choice == 'Y':
                return
            else:
                sys.exit()

    truth = truthOut.readline()
    if truth != '':
        print ('Problem with workload ' + workload + ', query ' + query + ' and outliers_pct ' + str(outliers_pct))
        print ('Search produced a subset of the ground truth results')
        print ('Example of missing true result ' + repr(truth))
        print ('Type Y to continue')
        choice = raw_input()
        if choice == 'Y':
            return
        else:
            sys.exit()

    print ('Workload ' + workload + ' and query ' + query + ' give correct canonical results! Checked ' + str(
        len(search)) + ' results')


################ MAIN ################

def main():
    f_perf = open('perf_numbers.txt', 'w')
    f_perf.write('QueryId , Qfrag , GJ\n')
    for configuration in configurations:
        global workloads
        workloads = configuration[0]
        global queriesAnchored
        queriesAnchored = configuration[1]
        global queriesUnanchored
        queriesUnanchored = configuration[2]
        global outliers_pcts
        outliers_pcts = configuration[3]
        global numServers
        numServers = configuration[4]
        global numPartitionsPerServer
        numPartitionsPerServer = configuration[5]
        global outputActive
        outputActive = configuration[6]

        writeClusterYaml(numServers, numPartitionsPerServer, outputActive)

        if os.path.isdir('output-search'):
            shutil.rmtree('output-search', ignore_errors=False, onerror=None)

        outliers_pct = str(0.0)
        for workload in workloads:
            x = []
            y = []
            y_gj = []
            for queryId in queriesAnchored:
                x.append(queryId)
                if doRun:
                    perf = run(workload, queryId, outliers_pct, minMatches, True, gj=False)
                    y.append(float(perf)/1000)
                    perf_gj = run(workload, queryId, outliers_pct, minMatches, True, gj=True)
                    y_gj.append(float(perf_gj)/1000)
                    f_perf.write(str(queryId) + ' , ' + str(perf) + ' , ' + str(perf_gj) + '\n')
                if doVerify and outputActive and outputBinary == 'false':
                    print('Running verification for workload ' + workload + ' query Q' + str(
                        queryId) + ' outliers_pct ' + outliers_pct)
                    verifyGJ(workload, queryId)
                    #verifyAnchored(workload,queryId)
                    #verify(workload, queryId, outliers_pct, True)
                else:
                    print ('Skipping verification for workload ' + workload + ' query Q' + str(
                        queryId) + ' outliers_pct ' + outliers_pct)
            plt.plot(x, y, '-b', label='Qfrag')
            plt.plot(x, y_gj, '-r', label='GJ')
            plt.legend(loc='upper right')
            plt.show()
    f_perf.close()
        # for workload in workloads:
        #     for queryId in queriesUnanchored:
        #         for outliers_pct in outliers_pcts:
        #             if doRun:
        #                 #run(workload, queryId, outliers_pct, minMatches, False, gj=False)
        #                 run(workload, queryId, outliers_pct, minMatches, False, gj=True)
        #             if doVerify and outputActive and outputBinary == 'false':
        #                 print ('Running verification for workload ' + workload + ' query Q' + str(
        #                     queryId) + 'u outliers_pct ' + outliers_pct)
        #                 #verifyGJ(workload, queryId, False)
        #                 verifyAnchored(workload, queryId, anchored=False)
        #                 # if workload == 'citeseer':
        #                 #     verify('citeseer', queryId, outliers_pct, False)
        #             else:
        #                 print ('Skipping verification for workload ' + workload + ' query Q' + str(
        #                     queryId) + 'u outliers_pct ' + outliers_pct)

main()
