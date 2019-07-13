package client;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.lf5.LogLevel;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;

import quickfix.FileLogFactory;
import quickfix.FileStoreFactory;
import quickfix.Initiator;

import quickfix.LogFactory;
import quickfix.MessageStoreFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;

import quickfix.fix44.MessageFactory;

public class FXClient {

	public FXClient() {
	}

	private static final Logger LOG = Logger.getLogger(UBSFXClient.class.getName());
	private static String entityID = "";
	private static String ppID = "";
	private static SessionID sessionID;
	static Initiator initiator;
	private static javax.jms.Connection connection=null;
	private static Queue request_Queue=null;
	private static javax.jms.Session Request_Session=null;
	private static final ApplicationImpl application = new ApplicationImpl();

	/**
	 * @param args
	 *
	 * Command line args : 0. Properties File path
	 * eg : "..\\Dat\\FX\\Config.properties"
	 */

	public static void main(String[] args) {
		BasicConfigurator.configure(); //enough for configuring log4j
		Logger.getRootLogger().setLevel(Level.INFO);
		FXClient client = new FXClient();
		client.initializeParams(args);
		client.startClient();
	}

	/**
	 *  Establishes login to the FX server and starts RequestQuote and OrderProcessing thread.
	 */
	private void startClient() {
		//--------------------------------------------------------------------
		// Create a JVMShutdownHook to trap jvm exit event
		final JVMShutdownHook jvmShutdownHook = new JVMShutdownHook();
		Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
		//--------------------------------------------------------------------
		if (login()) {
			try {
				try {

					Request_Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					request_Queue=Request_Session.createQueue(ConfigReader.getConfigFile().getProperty("Request_Queue"));
					MessageConsumer messageConsumer = Request_Session.createConsumer(request_Queue);
					messageConsumer.setMessageListener(new RequestQuote());
					connection.start();
					LOG.info("Connection started successfully.Listening for Messages Request...");
				} catch(Exception e) {
					LOG.error("Error while creating session!"+e.getMessage());
				}

			} catch (Exception e) {
				LOG.error("Error in startClient : Error while staring threads :: " + e.getMessage());
			}
		} else {
			LOG.info("Unable to login to FX FIX Server");
			return;
		}
	}

	/**
	 * Starts the intiator and establishes connection with the FIX server.
	 *
	 * @return true if login successful else false
	 */

	public static boolean login() {
		try {
			LOG.info("Attempting login ...");
			final SessionSettings settings = new SessionSettings(ConfigReader.getConfigFile().getProperty("Initiator"));
			final String beginString = settings.get().getString("BeginString");
			final String senderCompID = settings.get().getString("SenderCompID");
			final String targetCompID = settings.get().getString("TargetCompID");
			sessionID = new SessionID(beginString, senderCompID, targetCompID);


			MessageStoreFactory storeFactory;
			LogFactory logFactory;

			storeFactory = new FileStoreFactory(settings);
			logFactory = new FileLogFactory(settings);

			MessageFactory messageFactory = new MessageFactory();
			initiator = new SocketInitiator(application, storeFactory, settings, logFactory, messageFactory);

			LOG.info("Initiate login request ...");
			int waitForLogon = 1000;
			try {
				waitForLogon = Integer.parseInt(ConfigReader.getConfigFile().getProperty("Wait_For_Logon"));
			} catch (Exception e) {
				LOG.info("Loaded default wait for logon :" + waitForLogon + "ms");
			}
						int loginAttempts;
						try {
							loginAttempts = Integer.parseInt(ConfigReader.getConfigFile().getProperty("Login_Attempts"));
						} catch (NumberFormatException nfe) {
							loginAttempts = -1;
						}
			initiator.start();
			Thread.sleep(waitForLogon);
			int attemptCount = 1;
						do {
							Thread.sleep(waitForLogon);
							if (ApplicationImpl.isLoggedOn()) {
								LOG.info("Logged in to UBS FX FIX Server");
								return true;
							}
							LOG.info("Login attempt " + attemptCount++ + " failed. Retrying...");
						} while (loginAttempts == -1 || loginAttempts >= attemptCount);
		} catch (Exception e) {
			LOG.error("Error in loginQuote : " + e.getMessage());
		}
		return false;
	}


	private void initializeParams(String[] args) {
		try {
			ConfigReader.readConfigs(args[UBSFXConstants.Params_FI.PROP_FILE_PATH]);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			try {
				// Display error message and wait for 15 seconds and exit
				Thread.sleep(15000);
			} catch (InterruptedException f) {
				f.getMessage();
			}
			System.exit(1); // Exit program if failed to load properties file
		}

		try {

			SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
			connectionFactory.setHost(ConfigReader.getConfigFile().getProperty("Solace_IP")+":"+ConfigReader.getConfigFile().getProperty("Solace_Port"));
			connectionFactory.setUsername(ConfigReader.getConfigFile().getProperty("Solace_Username"));
			connectionFactory.setPassword(ConfigReader.getConfigFile().getProperty("Solace_Password"));
			connectionFactory.setVPN(ConfigReader.getConfigFile().getProperty("Solace_VPN"));
			connection = connectionFactory.createConnection();
			//			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static SessionID getSessionID() {
		return sessionID;
	}

	public static String getEntityID() {
		return entityID;
	}

	public static String getPriceProvider_ID() {
		return ppID;
	}

	public static javax.jms.Connection getConnection() {
		return connection;
	}


	/**
	 * Traps a JVM shutdown event to send logout message
	 */
	class JVMShutdownHook extends Thread {
		public void run() {
			System.out.println("Exit process ... ");
			//			try {
			//				connection.close();
			//			} catch (JMSException e) {
			//				LOG.error("Error while closing AcitveMQ connection");
			//				e.printStackTrace();
			//			}
			initiator.stop();
		}
	}

}
