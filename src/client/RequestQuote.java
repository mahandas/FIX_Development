package client;


import client.FIX_MessageStructure.StructNewOrderSingle;
import client.FIX_MessageStructure.StructQuoteRequest;

import java.util.Date;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.CFICode;
import quickfix.field.ClOrdID;
import quickfix.field.Currency;
import quickfix.field.FutSettDate;
import quickfix.field.FutSettDate2;
import quickfix.field.HandlInst;
import quickfix.field.NoPartyIDs;
import quickfix.field.NoRelatedSym;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.OrderQty2;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.Price;
import quickfix.field.QuoteID;
import quickfix.field.QuoteReqID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix44.NewOrderSingle;
import quickfix.fix44.QuoteRequest;

import util.DateUtil;


public class RequestQuote implements MessageListener{

	public RequestQuote() {

	}

	private static final Logger LOG = Logger.getLogger(RequestQuote.class.getName());
	private static final Properties config = ConfigReader.getConfigFile();


	@Override
	public void onMessage(Message message) {

//		try {
//			message.acknowledge();
//		} catch (JMSException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		System.out.println("Message :"+message);
		MapMessage crs1=(MapMessage) message;
//		System.out.println("Message "+message);
		
		try {
			JsonObject crs = new Gson().fromJson(crs1.getStringProperty("DATA"), JsonObject.class);
			if(crs1.getStringProperty("MSGTYPE").equalsIgnoreCase("REQUEST"))
			{	System.out.println("Placing RFQ for ID"+crs.get("Request_ID").getAsString());
				sendRequestForQuotation(crs);
			}
			else if(crs1.getStringProperty("MSGTYPE").equalsIgnoreCase("ORDER"))
			{	System.out.println("Placing Order for Order ID"+crs.get("Order_ID").getAsString());
				placeOrder(crs);
			}
			else
				System.out.println("Invalid Message Type");
		} catch (Exception e)
		{
			
		}
	}
	
	public void sendRequestForQuotation(JsonObject crs) {


		StructQuoteRequest objQuoteRequest = new StructQuoteRequest();

		try {            

			objQuoteRequest.setQuoteReqID(crs.get("Request_ID").getAsString()) ;
			objQuoteRequest.setNoRelatedSym(1);	 
			objQuoteRequest.setSymbol(crs.get("Deal_Pair").getAsString();
			objQuoteRequest.setSecurityID(objQuoteRequest.getSymbol());
			objQuoteRequest.setCurrency(crs.get("Quoted_CCY").getAsString());

			if(crs.get("Deal_Type").getAsString().compareToIgnoreCase("SPOT")==0)
				objQuoteRequest.setCFICode("SPOT") ; 
			else if(crs.get("Deal_Type").getAsString().compareToIgnoreCase("OUTRIGHT")==0)
			{	
				objQuoteRequest.setCFICode("OUTRIGHT");
			}
			
			objQuoteRequest.setOrderQty(crs.get("Quoted_Amt").getAsDouble());
			objQuoteRequest.setExpireTime(DateUtil.getCurrentDateTime_Str());

			try {
				buildQuoteRequestAndSend(objQuoteRequest);
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("Error in buildQuoteRequestAndSend for ID : " + objQuoteRequest.getQuoteReqID() +
						" : " + e.getMessage());
			}
		} catch (Exception e) {
			LOG.error("Error in sendRequestForQuotation for RFQ_ID : " + objQuoteRequest.getQuoteReqID() +
					" : " + e.getMessage());

		} finally {
	
		objQuoteRequest = null;
		}




	}

	private void buildQuoteRequestAndSend(StructQuoteRequest objQuoteRequest) throws Exception {

		if (!ApplicationImpl.isLoggedOn()) {
			throw new Exception("Unable get logon response from FIX server..");
		}

		QuoteRequest quoteRequest = new QuoteRequest();
		quoteRequest.set(new QuoteReqID(objQuoteRequest.getQuoteReqID()));		//Set QuoteReqID
		NewOrderSingle.NoPartyIDs grpPartyID = new NewOrderSingle.NoPartyIDs();
		grpPartyID.setField(new PartyID(config.getProperty("PartyID")));		//Get the PartyID from properties file
		grpPartyID.setField(new PartyIDSource('D'));			
		grpPartyID.setField(new PartyRole(13));				
		quoteRequest.addGroup(grpPartyID);
		quoteRequest.set(new NoRelatedSym(1));
		QuoteRequest.NoRelatedSym noRelatedSym = new QuoteRequest.NoRelatedSym();
		noRelatedSym.set(new Symbol(objQuoteRequest.getSymbol())); 
		noRelatedSym.set(new CFICode(objQuoteRequest.getCFICode()));
		quoteRequest.setDouble(38,objQuoteRequest.getOrderQty());

		
		quoteRequest.setString(15,objQuoteRequest.getCurrency());
		if(config.getProperty("Streaming").equalsIgnoreCase("Y"))
			quoteRequest.setString(126,objQuoteRequest.getExpireTime());
	
		SessionID sessionID = UBSFXClient.getSessionID();
		if (sessionID != null && Session.doesSessionExist(sessionID)) {
			if (Session.sendToTarget(quoteRequest, sessionID)) { 
				LOG.info( "Quote request sent successfully for ID :" + objQuoteRequest.getQuoteReqID() );
			} else {
				throw new Exception("Error while sending quote.");
			}
		} else {
			throw new Exception("Session does not exist ... sessionId : "+sessionID);
		}
	}
	
	public void placeOrder(JsonObject crs) {


		StructNewOrderSingle objNewOrder = new StructNewOrderSingle();
		SessionID sessionID;

		try {

			if (!ApplicationImpl.isLoggedOn()) {
				throw new Exception("Unable get logon response from FX server..");
			}
			objNewOrder.setClOrdID(crs.get("Order_ID").getAsString());
			objNewOrder.setQuoteID(crs.get("Quote_ID").getAsString()); // Generated by FIX server

			objNewOrder.setSymbol(crs.get("Symbol").getAsString());
			objNewOrder.setOrderQty(crs.get("OrderQty").getAsDouble());
			objNewOrder.setSide((crs.get("Side")).getAsString().charAt(0)=='1'?Side.BUY:Side.SELL);
			objNewOrder.setPrice(crs.get("Price").getAsDouble());
			System.out.println("Price:"+objNewOrder.getPrice());
			objNewOrder.setCurrency(crs.get("Currency").getAsString());
			sessionID = UBSFXClient.getSessionID();
			if (sessionID != null && Session.doesSessionExist(sessionID)) {
				if (Session.sendToTarget(buildOrder(objNewOrder), sessionID) == true) {
					LOG.info("Order request sent successfully For Order No:" + objNewOrder.getClOrdID() + "");
				} else {
					throw new Exception("Error while sending Order");
				}
			} else {
				throw new Exception("Session does not exist ...");
			}
		} catch (Exception e) {
			LOG.error("Error in Order No : " + objNewOrder.getClOrdID() + " : " + e.getMessage());
		} 


	}
	private NewOrderSingle buildOrder(StructNewOrderSingle objNewOrder) throws Exception {
		try {
			NewOrderSingle newOrder = new NewOrderSingle();
			newOrder.set(new ClOrdID(objNewOrder.getClOrdID()));
			NewOrderSingle.NoPartyIDs grpPartyID = new NewOrderSingle.NoPartyIDs();
			grpPartyID.setField(new PartyID(config.getProperty("PartyID")));	
			grpPartyID.setField(new PartyIDSource('D'));		
			grpPartyID.setField(new PartyRole(13));	
			newOrder.addGroup(grpPartyID);
			newOrder.setField(new NoPartyIDs(1));
			newOrder.setField(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE)); 
			newOrder.setField(new Symbol(objNewOrder.getSymbol()));
			newOrder.set(new Side(objNewOrder.getSide()));
			newOrder.set(new TransactTime(new Date()));
			newOrder.set(new OrderQty(objNewOrder.getOrderQty()));
			newOrder.set(new OrdType(OrdType.PREVIOUSLY_QUOTED));
			newOrder.set(new Price(objNewOrder.getPrice()));
			newOrder.set(new QuoteID(objNewOrder.getQuoteID()));
			newOrder.set(new Currency(objNewOrder.getCurrency()));
			return newOrder;
		} catch (Exception e) {
			throw new Exception("Error in buildOrderAndSend() : " + e.getMessage());
		}
	}

}
