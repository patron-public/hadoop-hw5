package com.training.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

public class Client {
	private static final Logger LOG = Logger.getLogger(Client.class.getName());
	private Configuration conf;
	private YarnClient yarnClient;
	private String appName;
	private int amPriority;
	private String amQueue = "";
	private int amMemory;
	private String appJar = "";
	private final String appMasterMainClass = ApplicationMaster.class.getName();
	private int priority;
	private int containerMemory;
	private int numContainers;
	private String adminUser;
	private String adminPassword;
	private String jbossAppUri;
	private String log4jPropFile = "";
	boolean debugFlag = false;
	private Options opts;

	public Client() {
		this.conf = new YarnConfiguration();
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		opts = new Options();
		opts.addOption("appname", true, "Application Name. Default value - JBoss on YARN");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
		opts.addOption("timeout", true, "Application timeout in milliseconds");
		opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
		opts.addOption("jar", true, "JAR file containing the applicationmaster");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
		opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
		opts.addOption("admin_user", true, "User id for initial administrator user");
		opts.addOption("admin_password", true, "Password for initial administrator user");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
	}

	public static void main(String[] args) {
		boolean result = false;
		try {
			Client client = new Client();
			try {
				boolean doRun = client.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				client.printUsage();
				System.exit(-1);
			}
			result = client.run();
		} catch (Throwable t) {
			System.exit(1);
		}
		if (result) {
			System.exit(0);
		}
		System.exit(2);
	}

	public boolean run() throws YarnException, IOException {
		yarnClient.start();
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
		if (amMemory > maxMem) {
			amMemory = maxMem;
		}
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();
		appContext.setApplicationName(appName);
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path(appJar);
		String pathSuffix = appName + File.separator + appId.getId() + File.separator + "JBossApp.jar";
		Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
		jbossAppUri = dst.toUri().toString();
		fs.copyFromLocalFile(false, true, src, dst);
		FileStatus destStatus = fs.getFileStatus(dst);
		LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
		amJarRsrc.setType(LocalResourceType.FILE);
		amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
		amJarRsrc.setTimestamp(destStatus.getModificationTime());
		amJarRsrc.setSize(destStatus.getLen());
		localResources.put("JBossApp.jar", amJarRsrc);
		amContainer.setLocalResources(localResources);
		Map<String, String> env = new HashMap<String, String>();
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
				.append("./*");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());
		amContainer.setEnvironment(env);
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		vargs.add("-Xmx" + amMemory + "m");
		vargs.add(appMasterMainClass);
		vargs.add("--container_memory " + String.valueOf(containerMemory));
		vargs.add("--num_containers " + String.valueOf(numContainers));
		vargs.add("--priority " + String.valueOf(priority));
		vargs.add("--admin_user " + adminUser);
		vargs.add("--admin_password " + adminPassword);
		vargs.add("--jar " + jbossAppUri);
		if (debugFlag) {
			vargs.add("--debug");
		}
//		vargs.add("1>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR + "/JBossApplicationMaster.stdout");
//		vargs.add("2>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR + "/JBossApplicationMaster.stderr");
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		amContainer.setCommands(commands);
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(amMemory);
		appContext.setResource(capability);
		appContext.setAMContainerSpec(amContainer);
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(amPriority);
		appContext.setPriority(pri);
		appContext.setQueue(amQueue);
		yarnClient.submitApplication(appContext);
		return monitorApplication(appId);
	}

	private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			ApplicationReport report = yarnClient.getApplicationReport(appId);
			LOG.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToAMToken="
					+ report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost="
					+ report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort()
					+ ", appStartTime=" + report.getStartTime() + ", yarnAppState="
					+ report.getYarnApplicationState().toString() + ", distributedFinalState="
					+ report.getFinalApplicationStatus().toString() + ", appTrackingUrl=" + report.getTrackingUrl()
					+ ", appUser=" + report.getUser());
			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus jbossStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == jbossStatus) {
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					return true;
				} else {
					LOG.info("Application did finished unsuccessfully." + " YarnState=" + state.toString()
							+ ", JBASFinalStatus=" + jbossStatus.toString() + ". Breaking monitoring loop");
					return false;
				}
			} else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
				LOG.info("Application did not finish." + " YarnState=" + state.toString() + ", JBASFinalStatus="
						+ jbossStatus.toString() + ". Breaking monitoring loop");
				return false;
			}
		}
	}

	public boolean init(String[] args) throws ParseException {
		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize");
		}
		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}
		if (cliParser.hasOption("debug")) {
			debugFlag = true;
		}
		appName = cliParser.getOptionValue("appname", "JBoss on YARN");
		amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		amQueue = cliParser.getOptionValue("queue", "default");
		amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "1024"));

		if (amMemory < 0) {
			throw new IllegalArgumentException(
					"Invalid memory specified for application master exiting." + " Specified memory=" + amMemory);
		}
		if (!cliParser.hasOption("jar")) {
			throw new IllegalArgumentException("No jar file specified for application master");
		}
		appJar = cliParser.getOptionValue("jar");
		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "1024"));
		numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "2"));
		adminUser = cliParser.getOptionValue("admin_user", "yarn");

		adminPassword = cliParser.getOptionValue("admin_password", "yarn");
		if (containerMemory < 0 || numContainers < 1) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory specified, exiting."
					+ " Specified containerMemory=" + containerMemory + ", numContainer=" + numContainers);
		}
		return true;
	}

	private void printUsage() {
		// TODO Auto-generated method stub

	}
}
