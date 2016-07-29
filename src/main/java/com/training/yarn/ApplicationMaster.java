package com.training.yarn;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ApplicationMaster {
	private static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getName());
	private Configuration conf;
	private AMRMClientAsync resourceManager;
	private NMClientAsync nmClientAsync;
	private NMCallbackHandler containerListener;
	private ApplicationAttemptId appAttemptID;
	private String appMasterHostname = "";
	private int appMasterRpcPort = 0;
	private String appMasterTrackingUrl = "";
	private int numTotalContainers;
	private int containerMemory;
	private int requestPriority;
	private String adminUser;
	private String adminPassword;
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	private AtomicInteger numRequestedContainers = new AtomicInteger();
	private Map<String, String> shellEnv = new HashMap<String, String>();
	private String jbossHome;
	private String appJar;
	private String domainController;
	private volatile boolean done;
	private volatile boolean success;
	private List<Thread> launchThreads = new ArrayList<Thread>();
	private Options opts;

	public ApplicationMaster() {
		conf = new YarnConfiguration();
		opts = new Options();
		opts.addOption("admin_user", true, "User id for initial administrator user");
		opts.addOption("admin_password", true, "Password for initial administrator user");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
		opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
		opts.addOption("jar", true, "JAR file containing the application");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
	}

	public static void main(String[] args) {
		boolean result = false;
		try {
			ApplicationMaster appMaster = new ApplicationMaster();
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			result = appMaster.run();
		} catch (Throwable t) {
			LOG.log(Level.SEVERE, "Error running JBossApplicationMaster", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("Application Master completed successfully. exiting");
			System.exit(0);
		} else {
			LOG.info("Application Master failed. exiting");
			System.exit(2);
		}
	}

	public boolean init(String[] args) throws ParseException, IOException {
		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException("No args specified for application master to initialize");
		}
		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}
		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}
		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "1024"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "2"));
		adminUser = cliParser.getOptionValue("admin_user", "yarn");
		adminPassword = cliParser.getOptionValue("admin_password", "yarn");
		appJar = cliParser.getOptionValue("jar");
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException("Cannot run JBoss Application Master with no containers");
		}
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		return true;
	}

	public boolean run() throws YarnException, IOException {
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		resourceManager.init(conf);
		resourceManager.start();

		containerListener = new NMCallbackHandler();
		
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);
		int maxMem = response.getMaximumResourceCapability().getMemory();
		if (containerMemory > maxMem) {
			containerMemory = maxMem;
		}
		for (int i = 0; i < numTotalContainers; ++i) {
			ContainerRequest containerAsk = setupContainerAskForRM();
			resourceManager.addContainerRequest(containerAsk);
		}
		numRequestedContainers.set(numTotalContainers);
		while (!done) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException ex) {
			}
		}
		finish();
		return success;
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		public void onContainersCompleted(
				List<ContainerStatus> completedContainers) {
				for (ContainerStatus containerStatus :
				completedContainers) {
				assert (containerStatus.getState() == ContainerState.COMPLETE);
				int exitStatus =
				containerStatus.getExitStatus();
				if (0 != exitStatus) {
					if (ContainerExitStatus.ABORTED != exitStatus) {
					numCompletedContainers.incrementAndGet();
					numFailedContainers.incrementAndGet();
					} else {
					numAllocatedContainers.decrementAndGet();
					numRequestedContainers.decrementAndGet();
					}
					} else {
					numCompletedContainers.incrementAndGet();
					}
					}
					int askCount = numTotalContainers –
					numRequestedContainers.get();
					numRequestedContainers.addAndGet(askCount);
					if (askCount > 0) {
					for (int i = 0; i < askCount; ++i) {
					ContainerRequest containerAsk =
					setupContainerAskForRM();
					resourceManager.
					addContainerRequest(containerAsk);
					}
					}
					if (numCompletedContainers.get() ==
					numTotalContainers) {
					done = true;
					}
					}

		public void onContainersAllocated(List<Container> allocatedContainers) {
			numAllocatedContainers.addAndGet(allocatedContainers.size());
			for (Container allocatedContainer : allocatedContainers) {
				LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener);
				Thread launchThread = new Thread(runnableLaunchContainer);
				launchThreads.add(launchThread);
				launchThread.start();
			}
		}

		public void onShutdownRequest() {
			done = true;
		}

		public void onNodesUpdated(List<NodeReport> updatedNodes) {
		}

		public void onError(Throwable e) {
			done = true;
			resourceManager.stop();
		}

		public float getProgress() {
			// TODO Auto-generated method stub
			return 0;
		}

	}

	private class LaunchContainerRunnable implements Runnable {
		Container container;
		NMCallbackHandler containerListener;

		public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener) {
			this.container = lcontainer;
			this.containerListener = containerListener;
		}

		public void run() {
			String containerId = container.getId().toString();
			ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			String applicationId = container.getId().getApplicationAttemptId().getApplicationId().toString();
			try {
				FileSystem fs = FileSystem.get(conf);
				LocalResource jbossDist = Records.newRecord(LocalResource.class);
				jbossDist.setType(LocalResourceType.ARCHIVE);
				jbossDist.setVisibility(LocalResourceVisibility.APPLICATION);
				Path jbossDistPath = new Path(new URI(JBossConstants.JBOSS_DIST_PATH));
				jbossDist.setResource(ConverterUtils.getYarnUrlFromPath(jbossDistPath));
				jbossDist.setTimestamp(fs.getFileStatus(jbossDistPath).getModificationTime());
				jbossDist.setSize(fs.getFileStatus(jbossDistPath).getLen());
				localResources.put(JBossConstants.JBOSS_SYMLINK, jbossDist);
				LocalResource jbossConf = Records.newRecord(LocalResource.class);
				jbossConf.setType(LocalResourceType.FILE);
				jbossConf.setVisibility(LocalResourceVisibility.APPLICATION);
				Path jbossConfPath = new Path(new URI(appJar));
				jbossConf.setResource(ConverterUtils.getYarnUrlFromPath(jbossConfPath));
				jbossConf.setTimestamp(fs.getFileStatus(jbossConfPath).getModificationTime());
				jbossConf.setSize(fs.getFileStatus(jbossConfPath).getLen());
				localResources.put(JBossConstants.JBOSS_ON_YARN_APP, jbossConf);
			} catch (Exception e) {
				LOG.log(Level.SEVERE, "Problem setting local resources", e);
				numCompletedContainers.incrementAndGet();
				numFailedContainers.incrementAndGet();
				return;
			}
			ctx.setLocalResources(localResources);
			List<String> commands = new ArrayList<String>();
			String host = container.getNodeId().getHost();
			String containerHome = conf.get("yarn.nodemanager.local-dirs") + File.separator + ContainerLocalizer.USERCACHE + File.separator
					+ System.getenv().get(Environment.USER.toString()) + File.separator + ContainerLocalizer.APPCACHE + File.separator + applicationId
					+ File.separator + containerId;
			jbossHome = containerHome + File.separator + JBossConstants.JBOSS_SYMLINK + File.separator + JBossConstants.JBOSS_VERSION;
			String jbossPermissionsCommand = String.format("chmod -R 777 %s", jbossHome);
			int portOffset = 0;
			int containerCount = containerListener.getContainerCount();
			if (containerCount > 1) {
				portOffset = containerCount * 150;
			}
			String domainControllerValue;
			if (domainController == null) {
				domainControllerValue = host;
			} else {
				domainControllerValue = domainController;
			}
			String jbossConfigurationCommand = String
					.format("%s/bin/java -cp %s %s --home %s --server_group %s --server %s --port_offset %s --admin_user %s --admin_password %s --domain_controller %s --host %s",
							Environment.JAVA_HOME.$(),
							"/opt/hadoop-2.2.0/share/hadoop/common/lib/*" + File.pathSeparator + containerHome + File.separator
									+ JBossConstants.JBOSS_ON_YARN_APP,
							JBossConfiguration.class.getName(), jbossHome, applicationId, containerId, portOffset, adminUser, adminPassword,
							domainControllerValue, host);
			String jbossCommand = String.format(
					"%s%sbin%sdomain.sh -Djboss.bind.address=%s -Djboss.bind.address.management=%s -Djboss.bind.address.unsecure=%s", jbossHome, File.separator,
					File.separator, host, host, host);
			commands.add(jbossPermissionsCommand);
			commands.add(JBossConstants.COMMAND_CHAIN);
			commands.add(jbossConfigurationCommand);
			commands.add(JBossConstants.COMMAND_CHAIN);
			commands.add(jbossCommand);
			ctx.setCommands(commands);
			containerListener.addContainer(container.getId(), container);
			nmClientAsync.startContainerAsync(container, ctx);
		}
	}
}
