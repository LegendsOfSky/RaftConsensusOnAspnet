using Xunit.Sdk;
using Xunit.v3;


[assembly: Parallelization(MaxThreads = 8 , Algorithm = ParallelAlgorithm.Conservative)]
