using RaftConsensusOnAspnet.RaftConsensus.Core;
using RaftConsensusOnAspnet.RaftConsensus.Core.Messages;
using RaftConsensusOnAspnet.RaftConsensus.Core.Misc;
using RaftConsensusOnAspnet.RaftConsensus.Core.Models.LogEntries;
using System.CommandLine;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using RaftConsensusOnAspnet.RaftConsensus.Messaging;
using RaftConsensusOnAspnet.RaftConsensus.Models.Config.Clusters;


namespace RaftConsensusOnAspnet.RaftConsensus;

public class Program
{
    private static IHttpClientFactory? s_HttpClientFactory;
    private static ClusterInfo? s_ClusterInfo;
    private static RaftNode? s_RaftNode;


    public static int Main(string[] args)
    {
        #region Configurate ASP.NET
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        AddClusterConfigToBuilderConfiguration(builder);
        builder.Services.AddHealthChecks();
        builder.Services.AddAuthorization();        // Add services to the container.
        builder.Services.AddOpenApi();              // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
        builder.Services.AddEndpointsApiExplorer(); // <--+-< Add Swagger services
        builder.Services.AddSwaggerGen();  // <-----------+
        builder.Services.Configure<ClusterInfo>(builder.Configuration);
        builder.Services.AddHttpClient(AppendEntriesRequestSender.ClientName , client => { client.Timeout = TimeSpan.FromSeconds(3); });
        builder.Services.AddHttpClient(VoteRequestSender.ClientName , client => { client.Timeout = TimeSpan.FromSeconds(3); });
        WebApplication app = builder.Build();

        s_ClusterInfo = app.Services.GetRequiredService<IOptions<ClusterInfo>>().Value;
        s_HttpClientFactory = app.Services.GetRequiredService<IHttpClientFactory>();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
            app.UseSwagger();
            app.UseSwaggerUI();
        }
        app.UseHttpsRedirection();
        app.UseAuthorization();
        #endregion

        #region Create command args
        Option<bool> debugModeOption = new Option<bool>("--debug")
        {
            Description = "Save debug logs and override console output with debug logs." ,
            Recursive = true ,
        };
        Option<int> electionTimeoutIntervalOption = new Option<int>("--timeout-interval")
        {
            Description = "How long before new election begin if no heart beat received or not enough votes acquired." ,
            Recursive = true ,
            DefaultValueFactory = _ => 500 + Random.Shared.Next() % 500 ,
        };
        Option<int> heartBeatIntervalOption = new Option<int>("--heart-beat-interval")
        {
            Description = "Interval between each heart beat." ,
            Recursive = true ,
            DefaultValueFactory = _ => 100 + Random.Shared.Next() % 100 ,
        };
        Option<Guid> nodeIdOption = new Option<Guid>("--id")
        {
            Description = "Start raft node with this id." ,
            Recursive = true ,
            DefaultValueFactory = _ => Guid.NewGuid() ,
        };
        #endregion

        RootCommand rootCommand = new RootCommand()
        {
            Options =
            {
                debugModeOption ,
                electionTimeoutIntervalOption ,
                heartBeatIntervalOption ,
                nodeIdOption ,
            } ,
        };
        rootCommand.SetAction(
                async parseResult =>
                {
                    IConfigurationSection storage = builder.Configuration.GetSection("Storage");

                    #region Bind log outputs
                    string logPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory , storage["LogPath"] ?? "logs"));
                    Directory.CreateDirectory(logPath);
                    List<TraceListener> standardTraceListeners = [new AlignedTraceListener($"{logPath}/standard-{DateTime.Today:yyyy-MM-dd}.log")];
                    List<TraceListener> debugTraceListeners = [];
                    if (parseResult.GetValue(debugModeOption))
                    {
                        debugTraceListeners.Add(new AlignedTraceListener(Console.Out));
                        debugTraceListeners.Add(new AlignedTraceListener($"{logPath}/debug-{DateTime.Today:yyyy-MM-dd}.logs"));
                    }
                    else
                        standardTraceListeners.Add(new AlignedTraceListener(Console.Out));;
                    #endregion

                    #region Bind data storages
                    string dataPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, storage["DataPath"] ?? "data"));
                    Directory.CreateDirectory(dataPath);
                    #endregion

                    #region Bind config storages
                    string configPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, storage["DataPath"] ?? "configs"));
                    Directory.CreateDirectory(configPath);
                    #endregion

                    #region Resolve other command line args
                    int electionTimeoutInterval = parseResult.GetValue(electionTimeoutIntervalOption);
                    int heartBeatInterval = parseResult.GetValue(heartBeatIntervalOption);
                    Guid nodeId = parseResult.GetValue(nodeIdOption);
                    #endregion

                    #region Resolve Dictionary<Guid , int>? nodeIdToDebugPos
                    Dictionary<Guid , int> nodeIdToDebugPos = [];
                    foreach (NodeOption nodeOption in s_ClusterInfo.Nodes.OrderBy(nodeOption => nodeOption.NodeId))
                        nodeIdToDebugPos[nodeOption.NodeId] = nodeIdToDebugPos.Count;
                    #endregion

                    if (!s_ClusterInfo.Nodes.Exists(nodeOption => nodeOption.NodeId == nodeId))
                        throw new InvalidOperationException("This node does not fit into the cluster option.");

                    s_RaftNode = new RaftNode(
                            nodeId , electionTimeoutInterval , heartBeatInterval , 5 ,
                            dataPath , true , standardTraceListeners , debugTraceListeners , nodeIdToDebugPos
                        );

                    #region Setup request agent help raft node to send requests
                    (List<VoteRequestSender>  voteRequestSenders , List<AppendEntriesRequestSender> appendEntriesRequestSenders) = ([] , []);
                    foreach (NodeOption nodeOption in s_ClusterInfo.Nodes.Where(nodeOption => nodeOption.NodeId != s_RaftNode.NodeId))
                    {
                        VoteRequestSender voteRequestSender = new VoteRequestSender(s_HttpClientFactory , s_RaftNode , nodeOption.Ip , nodeOption.NodeId);
                        voteRequestSenders.Add(voteRequestSender);
                        s_RaftNode.SendVoteRequestToOtherNodes += voteRequestSender.SendVoteRequestAsync;

                        AppendEntriesRequestSender appendRequestSender = new AppendEntriesRequestSender(s_HttpClientFactory , s_RaftNode , nodeOption.Ip , nodeOption.NodeId);
                        appendEntriesRequestSenders.Add(appendRequestSender);
                        s_RaftNode.AppendEntriesToOtherNodes += appendRequestSender.SendAppendEntriesRequestAsync;
                    }
                    #endregion

                    #region Bind endpoint to bussiness logic
                    app.MapDelete("/api/node" , (IHostApplicationLifetime lifetime) =>  // WARNING: keep this API hide behind proxy
                        {
                            s_RaftNode?.Stop();
                            lifetime.StopApplication();
                            return Results.Ok("Shutting down");
                        })
                        .WithName("StopRaftNode");
                    app.MapPut(
                            "/api/node/entries/" ,
                            (Guid requestId , Guid requesterId , int requesterTerm ,
                             int previousLogIndex , int previousLogTerm , int leaderCommit , IReadOnlyList<LogEntry> entries)
                                => s_RaftNode.HandleAppendEntries(
                                        new AppendEntriesArgs
                                        {
                                            RequestId = requestId ,
                                            RequesterId = requesterId ,
                                            ReceiverId = nodeId ,
                                            RequesterTerm = requesterTerm ,
                                            PreviousLogIndex = previousLogIndex ,
                                            PreviousLogTerm = previousLogTerm ,
                                            LeaderCommit = leaderCommit ,
                                            Entries = entries ,
                                        }
                                    )
                        ).WithName("AppendEntries");
                    app.MapPost(
                            "/api/node/heart-beat-interval" , (int newValue) => s_RaftNode.SetHeartBeatInterval(newValue)
                        ).WithName("ModifyHeartBeatInterval");
                    app.MapPost(
                            "/api/node/election-timeout-interval" , (int newValue) => s_RaftNode.SetElectionTimeoutInterval(newValue)
                        ).WithName("ModifyElectionTimeoutInterval");
                    app.MapPatch(
                            "/api/node/vote" ,
                            (Guid requestId , Guid requesterId , int requesterTerm , int requesterLastLogTerm , int requesterLastLogIndex)
                                => s_RaftNode.HandleVoteRequest(
                                        new VoteRequestArgs
                                        {
                                            RequestId = requestId ,
                                            RequesterId = requesterId ,
                                            ReceiverId = nodeId ,
                                            RequesterTerm = requesterTerm ,
                                            RequesterLastLogTerm = requesterLastLogTerm ,
                                            RequesterLastLogIndex = requesterLastLogIndex ,
                                        }
                                    )
                        ).WithName("RequestVote");
                    #endregion

                    Task raftWorking = s_RaftNode.StartAsync();
                    Task appRunning = app.RunAsync();
                    await raftWorking;
                    await appRunning;
                }
            );

        return rootCommand.Parse(args).Invoke();
    }

    private static void AddClusterConfigToBuilderConfiguration(WebApplicationBuilder builder)
    {
        string contentRoot = builder.Environment.ContentRootPath;
        string clusterFile = Path.Combine(contentRoot , "configs" , "cluster.settings.json");
        string sampleFile  = Path.Combine(contentRoot ,  "configs" , "sample.cluster.settings.json");
        if (File.Exists(clusterFile))
            builder.Configuration.AddJsonFile("configs/cluster.settings.json" , optional: false , reloadOnChange: true);
        else if (File.Exists(sampleFile))
            builder.Configuration.AddJsonFile("configs/sample.cluster.settings.json" , optional: false , reloadOnChange: true);
        else
            Trace.TraceError("Missing cluster settings.");
    }
}
