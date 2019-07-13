package client;


import client.FIX_MessageStructure.StructDontKnowTrade;
import client.FIX_MessageStructure.StructExecutionReport;
import client.FIX_MessageStructure.StructQuote;
import client.FIX_MessageStructure.StructQuoteRequestReject;
import client.FIX_MessageStructure.StructMassQuoteAck;
import java.util.Properties;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.log4j.Logger;
import com.google.gson.Gson;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.AvgPx;
import quickfix.field.BidForwardPoints;
import quickfix.field.BidForwardPoints2;
import quickfix.field.BidPx;
import quickfix.field.BidSize;
import quickfix.field.BidSpotRate;
import quickfix.field.BidSwapPoints;
import quickfix.field.CFICode;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.Currency;
import quickfix.field.DKReason;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.FutSettDate;
import quickfix.field.LastForwardPoints;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LastSpotRate;
import quickfix.field.MaturityDate;
import quickfix.field.MsgType;
import quickfix.field.OfferForwardPoints;
import quickfix.field.OfferForwardPoints2;
import quickfix.field.OfferPx;
import quickfix.field.OfferSize;
import quickfix.field.OfferSpotRate;
import quickfix.field.OfferSwapPoints;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.Password;
import quickfix.field.Price;
import quickfix.field.QuoteID;
import quickfix.field.QuoteReqID;
import quickfix.field.SecurityID;
import quickfix.field.SecurityIDSource;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TimeInForce;
import quickfix.field.Username;
import quickfix.field.ValidUntilTime;
import util.DateUtil;


public class ApplicationImpl extends MessageCracker implements Application {

	
	
	public ApplicationImpl() {

	}
	private static final Logger LOG = Logger.getLogger(ApplicationImpl.class.getName());
	private static volatile boolean loggedOn;
	private static final Properties config = ConfigReader.getConfigFile();
	private static MessageProducer producer  = null;
	private static Queue response_Queue=null;
	private static javax.jms.Session Response_Session=null;
	private static final Gson gson = new Gson();

	@Override
	public void fromAdmin(quickfix.Message message, SessionID sessionID) {
		if (isMessageOfType(message, MsgType.LOGOUT)) 
			loggedOn = false;
		
	}


	@Override
	public void onCreate(SessionID arg0) {

	}

	@Override
	public void onLogon(SessionID arg0) {
		LOG.info("Logged in");
		loggedOn = true;
		try {

			Response_Session = FXClient.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
			response_Queue=Response_Session.createQueue(ConfigReader.getConfigFile().getProperty("Response_Queue"));
			producer = Response_Session.createProducer(response_Queue);
		
		} catch (JMSException e1) {
			LOG.error("Error while establishing connection with Solace");
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onLogout(SessionID arg0) {
		LOG.info("Logged out");
		loggedOn = false;
	}

	@Override
	public void toAdmin(Message message, SessionID arg1) {
		if(isMessageOfType(message,MsgType.LOGON)) {
		 try {
               
                    String password = "";
                    password = config.getProperty("Password").trim();
                    if (!password.isEmpty()) {
                        message.setField(new Password(password)); //Required
                    } else {
                        throw new Exception(" Password is not present in properties file!");
                    }
                
               
                    String userName = "";
                    userName = config.getProperty("Username").trim();
                    
                    if (!userName.isEmpty()) {
                        message.setField(new Username(userName)); //Required
                    } else {
                        throw new Exception(" Username is not present in properties file!");
                    }
                } catch (Exception e) {
                    LOG.error("Error while setting Username and Password");
                    e.printStackTrace();
                }
           
		}
	}

	@Override
	public void toApp(quickfix.Message message, SessionID sessionID) {

	}
	public static boolean isLoggedOn() {
		return loggedOn;
	}

	@Override
	protected void onMessage(Message message, SessionID sessionID)
			throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
		super.onMessage(message, sessionID);
	}
	//to parse different types of messages
	@Override
	public void fromApp(quickfix.Message message, SessionID sessionID) throws UnsupportedMessageType, FieldNotFound,
	IncorrectTagValue {
		try {
						
			if (isMessageOfType(message, MsgType.QUOTE))
				parseQuote(message);
			else if (isMessageOfType(message, MsgType.QUOTE_CANCEL)) 
				parseQuoteCancel(message);
			else if (isMessageOfType(message, MsgType.EXECUTION_REPORT)) 
				parseExecReport(message);
			else if (isMessageOfType(message, MsgType.DONT_KNOW_TRADE)) 
				parseDntKnwTradeMsg(message);
			else if (isMessageOfType(message, MsgType.MASS_QUOTE_ACKNOWLEDGEMENT)) 
				parseQuoteAck(message);
			else if (isMessageOfType(message, MsgType.QUOTE_REQUEST_REJECT)) 
				parseQuoteRequestReject(message);
			
		} catch (Exception e) {
			LOG.error("Error while parsing message!");
		}
	}
	private boolean isMessageOfType(quickfix.Message message, String type) {
		try {
			return type.equals(message.getHeader().getField(new MsgType()).getValue());
		} catch (FieldNotFound e) {
			LOG.error(e.getMessage());
			return false;
		}
	}

	private void parseQuote(Message message) {

		StructQuote quote = new StructQuote();
		try {
			quote.setQuoteReqID(message.getField(new QuoteReqID()).getValue());	//Set Quote Request ID

			quote.setQuoteID(message.getField(new QuoteID()).getValue());		//Set the Quote ID
			quote.setSymbol(message.getField(new Symbol()).getValue());
			quote.setCFICode(message.getField(new CFICode()).getValue());		//Set the CFICode,FFCPNO = Forward Outright ,RCSXXX = Spot 
			
			try {
				quote.setSecurityID(message.getField(new SecurityID()).getValue());
			}catch(FieldNotFound fnf)
			{
				quote.setSecurityID("");
			}
			try {
				quote.setSecurityIDSource(message.getField(new SecurityIDSource()).getValue());
			}catch(FieldNotFound fnf)
			{
				quote.setSecurityIDSource("");
			}
			try {
				quote.setBaseCcyDeliveryLocationType(message.getString(9102));
			}catch(FieldNotFound fnf)
			{
				quote.setBaseCcyDeliveryLocationType("");
			}
			try {
				quote.setCounterCcyDeliveryLocationType(message.getString(9103));
			}catch(FieldNotFound fnf)
			{
				quote.setCounterCcyDeliveryLocationType("");
			}
			
			try {
				quote.setMaturityDate(DateUtil.convertDateFormat(message.getField(new MaturityDate()).getValue(),
						DateUtil.DB_DATE_FORMAT));
			}catch(FieldNotFound fnf)
			{
				quote.setMaturityDate("");
			}
			try {
				quote.setFixingReference(message.getString(7075));
			}catch(FieldNotFound fnf)
			{
				quote.setFixingReference("");
			}
			
			//Set the Valid Until Time in proper format
			quote.setValidUntilTime(DateUtil.convertDateFormat(message.getField(new ValidUntilTime()).getValue().toString(),
					"EEE MMM dd HH:mm:ss z yyyy",
					DateUtil.DB_DATE_TIME_FORMAT));
			
			try {
				quote.setBidPx(message.getField(new BidPx()).getValue());		//Tag 132
			} catch (FieldNotFound fnf) {
				quote.setBidPx(0);
			}
			try {
				quote.setOfferPx(message.getField(new OfferPx()).getValue());	//Tag 133
			} catch (FieldNotFound fnf) {
				quote.setOfferPx(0);
			}
			quote.setBidSize(message.getField(new BidSize()).getValue());		//Tag 134
			quote.setOfferSize(message.getField(new OfferSize()).getValue());	//Tag 135

			try {
				quote.setBidSpotRate(message.getField(new BidSpotRate()).getValue());	//tag 188
			} catch (FieldNotFound fnf) {
				quote.setBidSpotRate(0);
			}
			try {
				quote.setOfferSpotRate(message.getField(new OfferSpotRate()).getValue());	//tag 190
			} catch (FieldNotFound fnf) {

				quote.setOfferSpotRate(0);
			}
			try {
				quote.setBidForwardPoints(message.getField(new BidForwardPoints()).getValue());	//tag 189
			}catch (FieldNotFound fnf) {
				quote.setBidForwardPoints(0);

			}
			try {
				quote.setBidForwardPoints2(message.getField(new BidForwardPoints2()).getValue());	//Used in Swap Far leg
			}catch (FieldNotFound fnf) {
				quote.setBidForwardPoints2(0);

			}
			try {
				quote.setOfferForwardPoints(message.getField(new OfferForwardPoints()).getValue());	//tag 191
			}catch (FieldNotFound fnf) {
				quote.setOfferForwardPoints(0);

			}
			try {
				quote.setOfferForwardPoints2(message.getField(new OfferForwardPoints2()).getValue()); //Used in Swap Far leg
			}catch (FieldNotFound fnf) {
				quote.setOfferForwardPoints2(0);

			}
			try {
				quote.setFutSettDate(DateUtil.convertDateFormat(message.getField(new FutSettDate()).getValue(),	
						DateUtil.DB_DATE_FORMAT));			//Tag 64,Value Date of a Forward
			}catch(FieldNotFound fnf)
			{
				quote.setFutSettDate("");
			}
			quote.setCurrency(message.getField(new Currency()).getValue());
			try {
				quote.setExpireTime(DateUtil.convertDateFormat(message.getField(new ValidUntilTime()).getValue().toString(),
						"EEE MMM dd HH:mm:ss z yyyy",
						DateUtil.DB_DATE_TIME_FORMAT));
			}catch(FieldNotFound fnf)
			{
				quote.setExpireTime("");
			}
			try {
				quote.setBidSwapPoints(message.getField(new BidSwapPoints()).getValue());
			}catch(FieldNotFound fnf)
			{
				quote.setBidSwapPoints(0);
			}
			try{
				quote.setOfferSwapPoints(message.getField(new OfferSwapPoints()).getValue());
			}catch(FieldNotFound fnf)
			{
				quote.setOfferSwapPoints(0);
			}
			LOG.info("Quote Received for RFQ ID:"+quote.getQuoteReqID());
			String Quote=gson.toJson(quote);
			MapMessage JSONString=Response_Session.createMapMessage();
			JSONString.setStringProperty("DATA", Quote);
			JSONString.setStringProperty("MSGTYPE", "QUOTE");
			JSONString.setStringProperty("PPID", "BANK");
			LOG.info("JSON Quote :"+Quote);
			LOG.info("Message Quote :"+JSONString);
			//			javax.jms.MapMessage message = session.createMapMessage();
			producer.send(JSONString);

			LOG.info("Quote sent to Queue Successfully!");
		}catch(Exception e) {

			LOG.error("Error in parseQuote for  ID : " + quote.getQuoteReqID() + " : " + e.getMessage());
			
		}

	}
	private void parseQuoteRequestReject(Message message) {
		
		StructQuoteRequestReject quoteRequestRej=new StructQuoteRequestReject();
		
		try {
			quoteRequestRej.setQuoteReqID(message.getString(131));
			try {
				quoteRequestRej.setQuoteRequestRejectReason(message.getString(658));
			}catch(FieldNotFound fnf)
			{
				quoteRequestRej.setQuoteRequestRejectReason("");
			}
			
			quoteRequestRej.setNoRelatedSym(message.getInt(146));

			try {
			quoteRequestRej.setSymbol(message.getField(new Symbol()).getValue());				
			}catch(FieldNotFound fnf)
			{
				quoteRequestRej.setSymbol("");
			}
		

			try {
				quoteRequestRej.setText(message.getField(new Text()).getValue());
			}catch(FieldNotFound fnf)
			{
				quoteRequestRej.setText(message.getField(new Text()).getValue());
			}
			LOG.info("QuoteRequest Rejected received for ID :"+quoteRequestRej.getQuoteReqID());
			LOG.info("Reason:"+quoteRequestRej.getText());

		}catch(Exception e)
		{
			LOG.error("Error in parseQuoteRequestReject  for ID : " + quoteRequestRej.getQuoteReqID() );

		}

	}

		private void parseQuoteAck(quickfix.Message ackMsg) {
	        StructMassQuoteAck quoteAck = new StructMassQuoteAck();
	        try {
	            try {
	                quoteAck.setText(ackMsg.getField(new Text()).getValue());
	            } catch (FieldNotFound fnf) {
	                quoteAck.setText("");
	            }
	            try {
	                quoteAck.setQuoteAckStatus(ackMsg.getString(297));
	            } catch (FieldNotFound fnf) {
	                quoteAck.setQuoteAckStatus("");
	            }
	            try {
	                quoteAck.setTradingSessionId(ackMsg.getString(336));
	            } catch (FieldNotFound fnf) {
	                quoteAck.setTradingSessionId("");
	            }
	            try {
	                quoteAck.setQuoteRejectReason(ackMsg.getString(300));
	            } catch (FieldNotFound fnf) {
	                quoteAck.setQuoteRejectReason("");
	            }
	            quoteAck.setQuoteReqID(ackMsg.getString(131));
	

	        } catch (Exception e) {
	            LOG.error("Error in parseQuoteAck  for ID : " + quoteAck.getQuoteReqID() + " : " + e.getMessage());
	        }
	    }


	private void parseDntKnwTradeMsg(Message message) {
		
		StructDontKnowTrade dntKnowTrade=new StructDontKnowTrade();
		try {
			dntKnowTrade.setOrderID(message.getField(new OrderID()).getValue());
			dntKnowTrade.setExecID(message.getField(new OrderID()).getValue());
			
			dntKnowTrade.setDKReason(message.getField(new DKReason()).getValue());
			dntKnowTrade.setSymbol(message.getField(new Symbol()).getValue());
			dntKnowTrade.setOrderQty(message.getField(new OrderQty()).getValue());
			dntKnowTrade.setText(message.getField(new Text()).getValue());
			dntKnowTrade.setSide(message.getField(new Side()).getValue());
			
			
		}catch (Exception e) {
			LOG.error("Error in parseDntKnwTradeMsg for Order ID : " + dntKnowTrade.getOrderID() + " : " + e.getMessage());
		}
	}

	
	
	private void parseExecReport(Message execRptMsg) {
		

		StructExecutionReport execRpt = new StructExecutionReport();
		try {
			execRpt.setOrderID(execRptMsg.getField(new OrderID()).getValue());		//Set the Order Id in the Execution Report Structure
			execRpt.setClOrdID(execRptMsg.getField(new ClOrdID()).getValue());		//Set the Client Order ID
			execRpt.setOrdStatus(execRptMsg.getField(new OrdStatus()).getValue());
			execRpt.setExecID(execRptMsg.getField(new ExecID()).getValue());
			execRpt.setExecType(execRptMsg.getField(new ExecType()).getValue());
			execRpt.setOrdStatus(execRptMsg.getField(new OrdStatus()).getValue());


			if((execRptMsg.getField(new ExecType()).getValue())==ExecType.TRADE)		
			{
				execRpt.setLastPx(execRptMsg.getField(new LastPx()).getValue());
				execRpt.setLastSpotRate(execRptMsg.getField(new LastSpotRate()).getValue());
				try {
					execRpt.setLastForwardPoints(execRptMsg.getField(new LastForwardPoints()).getValue());
				}catch(FieldNotFound fnf)
				{
					execRpt.setLastForwardPoints(0);
				}
			}

			try {
				execRpt.setSymbol(execRptMsg.getField(new Symbol()).getValue());	//Set the Currency Pair,e.g:EUR/USD
			} catch (FieldNotFound fnf) {
				execRpt.setSymbol("");
			}

			execRpt.setCFICode(execRptMsg.getField(new CFICode()).getValue());		 
			execRpt.setSide(execRptMsg.getField(new Side()).getValue());			
			execRpt.setOrderQty(execRptMsg.getField(new OrderQty()).getValue());
			
			try {
				execRpt.setOrdType(execRptMsg.getField(new OrdType()).getValue());
			} catch (FieldNotFound fnf) {
				LOG.info(fnf.getMessage());
			}
			try {
				execRpt.setPrice(execRptMsg.getField(new Price()).getValue());
			} catch (FieldNotFound fnf) {
				LOG.info(fnf.getMessage());
			}
			try {
				execRpt.setCurrency(execRptMsg.getField(new Currency()).getValue());
			} catch (FieldNotFound fnf) {
				LOG.info(fnf.getMessage());
			}

			try {
				execRpt.setTimeInForce(execRptMsg.getField(new TimeInForce()).getValue());
			} catch (FieldNotFound fnf) {
				LOG.info(fnf.getMessage());
			}
			try {
				execRpt.setLastQty(execRptMsg.getField(new LastQty()).getValue());
			}catch(FieldNotFound fnf)
			{
				execRpt.setLastQty(0);
			}
			
			execRpt.setCumQty(execRptMsg.getField(new CumQty()).getValue());

			if((execRptMsg.getField(new OrdStatus()).getValue())==OrdStatus.CANCELED || (execRptMsg.getField(new OrdStatus()).getValue())!=OrdStatus.REJECTED)
				execRpt.setLeavesQty(0);
			else
				execRpt.setLeavesQty(execRpt.getOrderQty()-execRpt.getCumQty());
			
			try {
				execRpt.setAvgPx(execRptMsg.getField(new AvgPx()).getValue());
			}catch(FieldNotFound fnf) {
				execRpt.setAvgPx(0);
			}
			try {
				execRpt.setPrice(execRptMsg.getField(new Price()).getValue());
			}catch(FieldNotFound fnf) {
				execRpt.setPrice(0);
			}
			
			try {
				execRpt.setText(execRptMsg.getField(new Text()).getValue());
			} catch (FieldNotFound fnf) {
				execRpt.setText("");
			}
			LOG.info("Execution Report received for Order ID:"+execRpt.getClOrdID());
			String execJSON=gson.toJson(execRpt);
			MapMessage JSONString=Response_Session.createMapMessage();

			JSONString.setStringProperty("DATA", execJSON);
			JSONString.setStringProperty("MSGTYPE", "EXECUTION_REPORT");
			JSONString.setStringProperty("PPID", "BANK");
			
			LOG.info("JSON Quote :"+execJSON);
			producer.send(JSONString);

		} catch (Exception e) {
			LOG.error("Error in parseExecReport for Order ID : " + execRpt.getClOrdID() + " : " + e.getMessage());
		}

	}

	private void parseQuoteCancel(Message message) {
		

	}
	
}
