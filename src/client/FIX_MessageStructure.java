package client;

public class FIX_MessageStructure {

	public static class StructQuoteRequest{
		
		private String QuoteReqID;
		private int NoRelatedSym;
		private int NoPartyIDs;
        private String PartyID;
        private char PartyIDSource;
        private int PartyRole;
        private String Symbol;
        private String CFICode;
        private String SecurityID;
        private String SecurityIDSource;
        private String BaseCcyDeliveryLocationType;
        private String CounterCcyDeliveryLocationType;
		private double OrderQty;
        private double OrderQty2;
        private String SettlmntTyp;
        private String FutSettDate;
        private String SettlmntTyp2;
        private String FutSettDate2;
        private String Currency;
        private char RequestFixingInfo;
        private String ExpireTime;
        private String MaturityDate;
        
        
        
        public String getMaturityDate() {
			return MaturityDate;
		}
		public void setMaturityDate(String maturityDate) {
			MaturityDate = maturityDate;
		}
		public String getQuoteReqID() {
			return QuoteReqID;
		}
		public void setQuoteReqID(String quoteReqID) {
			QuoteReqID = quoteReqID;
		}
		public int getNoRelatedSym() {
			return NoRelatedSym;
		}
		public void setNoRelatedSym(int noRelatedSym) {
			NoRelatedSym = noRelatedSym;
		}
		
		public int getNoPartyIDs() {
			return NoPartyIDs;
		}
		public void setNoPartyIDs(int noPartyIDs) {
			NoPartyIDs = noPartyIDs;
		}
		public String getPartyID() {
			return PartyID;
		}
		public void setPartyID(String partyID) {
			PartyID = partyID;
		}
		public char getPartyIDSource() {
			return PartyIDSource;
		}
		public void setPartyIDSource(char partyIDSource) {
			PartyIDSource = partyIDSource;
		}
		public int getPartyRole() {
			return PartyRole;
		}
		public void setPartyRole(int partyRole) {
			PartyRole = partyRole;
		}
		public String getSymbol() {
			return Symbol;
		}
		public void setSymbol(String symbol) {
			Symbol = symbol;
		}
		public String getCFICode() {
			return CFICode;
		}
		public void setCFICode(String cFICode) {
			CFICode = cFICode;
		}
		public String getSecurityID() {
			return SecurityID;
		}
		public void setSecurityID(String securityID) {
			SecurityID = securityID;
		}
		public String getSecurityIDSource() {
			return SecurityIDSource;
		}
		public void setSecurityIDSource(String securityIDSource) {
			SecurityIDSource = securityIDSource;
		}
		public String getBaseCcyDeliveryLocationType() {
			return BaseCcyDeliveryLocationType;
		}
		public void setBaseCcyDeliveryLocationType(String baseCcyDeliveryLocationType) {
			BaseCcyDeliveryLocationType = baseCcyDeliveryLocationType;
		}
		public String getCounterCcyDeliveryLocationType() {
			return CounterCcyDeliveryLocationType;
		}
		public void setCounterCcyDeliveryLocationType(String counterCcyDeliveryLocationType) {
			CounterCcyDeliveryLocationType = counterCcyDeliveryLocationType;
		}
		public double getOrderQty() {
			return OrderQty;
		}
		public void setOrderQty(double orderQty) {
			OrderQty = orderQty;
		}
		public double getOrderQty2() {
			return OrderQty2;
		}
		public void setOrderQty2(double orderQty2) {
			OrderQty2 = orderQty2;
		}
		public String getSettlmntTyp() {
			return SettlmntTyp;
		}
		public void setSettlmntTyp(String settlmntTyp) {
			SettlmntTyp = settlmntTyp;
		}
		public String getFutSettDate() {
			return FutSettDate;
		}
		public void setFutSettDate(String futSettDate) {
			FutSettDate = futSettDate;
		}
		public String getSettlmntTyp2() {
			return SettlmntTyp2;
		}
		public void setSettlmntTyp2(String settlmntTyp2) {
			SettlmntTyp2 = settlmntTyp2;
		}
		public String getFutSettDate2() {
			return FutSettDate2;
		}
		public void setFutSettDate2(String futSettDate2) {
			FutSettDate2 = futSettDate2;
		}
		public String getCurrency() {
			return Currency;
		}
		public void setCurrency(String currency) {
			Currency = currency;
		}
		public char getRequestFixingInfo() {
			return RequestFixingInfo;
		}
		public void setRequestFixingInfo(char requestFixingInfo) {
			RequestFixingInfo = requestFixingInfo;
		}
		public String getExpireTime() {
			return ExpireTime;
		}
		public void setExpireTime(String expireTime) {
			ExpireTime = expireTime;
		}

        
	}
	public static class StructQuoteRequestReject{
		private String QuoteReqID;
		private String QuoteRequestRejectReason;
		private int NoRelatedSym;
		private String Symbol;
		private String CFICode;
		private String SecurityID;
		private String SecurityIDSource;
		private String BaseCcyDeliveryLocationType;
        private String CounterCcyDeliveryLocationType;
		private double OrderQty;
        private double OrderQty2;
        private String SettlmntTyp;
        private String FutSettDate;
        private String SettlmntTyp2;
        private String FutSettDate2;
        private String Currency;
        private String Text;
		public String getQuoteReqID() {
			return QuoteReqID;
		}
		public void setQuoteReqID(String quoteReqID) {
			QuoteReqID = quoteReqID;
		}
		public String getQuoteRequestRejectReason() {
			return QuoteRequestRejectReason;
		}
		public void setQuoteRequestRejectReason(String quoteRequestRejectReason) {
			QuoteRequestRejectReason = quoteRequestRejectReason;
		}
		public int getNoRelatedSym() {
			return NoRelatedSym;
		}
		public void setNoRelatedSym(int noRelatedSym) {
			NoRelatedSym = noRelatedSym;
		}
		public String getSymbol() {
			return Symbol;
		}
		public void setSymbol(String symbol) {
			Symbol = symbol;
		}
		public String getSecurityID() {
			return SecurityID;
		}
		public void setSecurityID(String securityID) {
			SecurityID = securityID;
		}
		public String getCFICode() {
			return CFICode;
		}
		public void setCFICode(String cFICode) {
			CFICode = cFICode;
		}
		public String getSecurityIDSource() {
			return SecurityIDSource;
		}
		public void setSecurityIDSource(String securityIDSource) {
			SecurityIDSource = securityIDSource;
		}
		public String getBaseCcyDeliveryLocationType() {
			return BaseCcyDeliveryLocationType;
		}
		public void setBaseCcyDeliveryLocationType(String baseCcyDeliveryLocationType) {
			BaseCcyDeliveryLocationType = baseCcyDeliveryLocationType;
		}
		public String getCounterCcyDeliveryLocationType() {
			return CounterCcyDeliveryLocationType;
		}
		public void setCounterCcyDeliveryLocationType(String counterCcyDeliveryLocationType) {
			CounterCcyDeliveryLocationType = counterCcyDeliveryLocationType;
		}
		public double getOrderQty() {
			return OrderQty;
		}
		public void setOrderQty(double orderQty) {
			OrderQty = orderQty;
		}
		public double getOrderQty2() {
			return OrderQty2;
		}
		public void setOrderQty2(double orderQty2) {
			OrderQty2 = orderQty2;
		}
		public String getSettlmntTyp() {
			return SettlmntTyp;
		}
		public void setSettlmntTyp(String settlmntTyp) {
			SettlmntTyp = settlmntTyp;
		}
		public String getFutSettDate() {
			return FutSettDate;
		}
		public void setFutSettDate(String futSettDate) {
			FutSettDate = futSettDate;
		}
		public String getSettlmntTyp2() {
			return SettlmntTyp2;
		}
		public void setSettlmntTyp2(String settlmntTyp2) {
			SettlmntTyp2 = settlmntTyp2;
		}
		public String getFutSettDate2() {
			return FutSettDate2;
		}
		public void setFutSettDate2(String futSettDate2) {
			FutSettDate2 = futSettDate2;
		}
		public String getCurrency() {
			return Currency;
		}
		public void setCurrency(String currency) {
			Currency = currency;
		}
		public String getText() {
			return Text;
		}
		public void setText(String text) {
			Text = text;
		}
        
		
	}
	 public static class StructQuote{         
		 private String QuoteReqID;
		 private String QuoteID;
		 private String Symbol;
	     private String CFICode;
	     private String SecurityID;
	     private String SecurityIDSource;
	     private String BaseCcyDeliveryLocationType;
	     private String CounterCcyDeliveryLocationType;
	     private String MaturityDate;
	     private String MaturityDate2;
	     private String FixingReference;
	     private String ValidUntilTime;
	     private double BidPx;
	     private double OfferPx;
	     private double BidSize;
	     private double OfferSize;
	     private double BidSpotRate;
	     private double OfferSpotRate;
	     private double BidForwardPoints;
	     private double BidForwardPoints2;
	     private double OfferForwardPoints;
	     private double OfferForwardPoints2;
	     private String FutSettDate;
	     private String FutSettDate2;
	     private Double OrderQty2;
	     private String Currency;
	     private String ExpireTime;
	     private double BidSwapPoints;
	     private double OfferSwapPoints;
	     
		public String getQuoteReqID() {
			return QuoteReqID;
		}
		public void setQuoteReqID(String quoteReqID) {
			QuoteReqID = quoteReqID;
		}
		public String getQuoteID() {
			return QuoteID;
		}
		public void setQuoteID(String quoteID) {
			QuoteID = quoteID;
		}
		public String getSymbol() {
			return Symbol;
		}
		public void setSymbol(String symbol) {
			Symbol = symbol;
		}
		public String getCFICode() {
			return CFICode;
		}
		public void setCFICode(String cFICode) {
			CFICode = cFICode;
		}
		public String getSecurityID() {
			return SecurityID;
		}
		public void setSecurityID(String securityID) {
			SecurityID = securityID;
		}
		public String getSecurityIDSource() {
			return SecurityIDSource;
		}
		public void setSecurityIDSource(String securityIDSource) {
			SecurityIDSource = securityIDSource;
		}
		public String getBaseCcyDeliveryLocationType() {
			return BaseCcyDeliveryLocationType;
		}
		public void setBaseCcyDeliveryLocationType(String baseCcyDeliveryLocationType) {
			BaseCcyDeliveryLocationType = baseCcyDeliveryLocationType;
		}
		public String getCounterCcyDeliveryLocationType() {
			return CounterCcyDeliveryLocationType;
		}
		public void setCounterCcyDeliveryLocationType(String counterCcyDeliveryLocationType) {
			CounterCcyDeliveryLocationType = counterCcyDeliveryLocationType;
		}
		public String getMaturityDate() {
			return MaturityDate;
		}
		public void setMaturityDate(String maturityDate) {
			MaturityDate = maturityDate;
		}
		public String getMaturityDate2() {
			return MaturityDate2;
		}
		public void setMaturityDate2(String maturityDate2) {
			MaturityDate2 = maturityDate2;
		}
		public String getFixingReference() {
			return FixingReference;
		}
		public void setFixingReference(String fixingReference) {
			FixingReference = fixingReference;
		}
		public String getValidUntilTime() {
			return ValidUntilTime;
		}
		public void setValidUntilTime(String validUntilTime) {
			ValidUntilTime = validUntilTime;
		}
		public double getBidPx() {
			return BidPx;
		}
		public void setBidPx(double bidPx) {
			BidPx = bidPx;
		}
		public double getOfferPx() {
			return OfferPx;
		}
		public void setOfferPx(double offerPx) {
			OfferPx = offerPx;
		}
		public double getBidSize() {
			return BidSize;
		}
		public void setBidSize(double bidSize) {
			BidSize = bidSize;
		}
		public double getOfferSize() {
			return OfferSize;
		}
		public void setOfferSize(double offerSize) {
			OfferSize = offerSize;
		}
		public double getBidSpotRate() {
			return BidSpotRate;
		}
		public void setBidSpotRate(double bidSpotRate) {
			BidSpotRate = bidSpotRate;
		}
		public double getOfferSpotRate() {
			return OfferSpotRate;
		}
		public void setOfferSpotRate(double offerSpotRate) {
			OfferSpotRate = offerSpotRate;
		}
		public double getBidForwardPoints() {
			return BidForwardPoints;
		}
		public void setBidForwardPoints(double bidForwardPoints) {
			BidForwardPoints = bidForwardPoints;
		}
		public double getBidForwardPoints2() {
			return BidForwardPoints2;
		}
		public void setBidForwardPoints2(double bidForwardPoints2) {
			BidForwardPoints2 = bidForwardPoints2;
		}
		public double getOfferForwardPoints() {
			return OfferForwardPoints;
		}
		public void setOfferForwardPoints(double offerForwardPoints) {
			OfferForwardPoints = offerForwardPoints;
		}
		public double getOfferForwardPoints2() {
			return OfferForwardPoints2;
		}
		public void setOfferForwardPoints2(double offerForwardPoints2) {
			OfferForwardPoints2 = offerForwardPoints2;
		}
		public String getFutSettDate() {
			return FutSettDate;
		}
		public void setFutSettDate(String futSettDate) {
			FutSettDate = futSettDate;
		}
		public String getFutSettDate2() {
			return FutSettDate2;
		}
		public void setFutSettDate2(String futSettDate2) {
			FutSettDate2 = futSettDate2;
		}
		public Double getOrderQty2() {
			return OrderQty2;
		}
		public void setOrderQty2(Double orderQty2) {
			OrderQty2 = orderQty2;
		}
		public String getCurrency() {
			return Currency;
		}
		public void setCurrency(String currency) {
			Currency = currency;
		}
		public String getExpireTime() {
			return ExpireTime;
		}
		public void setExpireTime(String expireTime) {
			ExpireTime = expireTime;
		}
		public double getBidSwapPoints() {
			return BidSwapPoints;
		}
		public void setBidSwapPoints(double bidSwapPoints) {
			BidSwapPoints = bidSwapPoints;
		}
		public double getOfferSwapPoints() {
			return OfferSwapPoints;
		}
		public void setOfferSwapPoints(double offerSwapPoints) {
			OfferSwapPoints = offerSwapPoints;
		}
	     	     
	 }
	 public static class StructNewOrderSingle {
	    private String ClOrdID;
	    private String MDReqID;
	    private int NoPartyIDs;
        private String PartyID;
        private char PartyIDSource;
        private int PartyRole;
	    private String FutSettDate;
	    private String FutSettDate2;
	    private char HandlInst;
	    private String Symbol;
	    private String CFICode;
	    private String SecurityID;
	    private String SecurityIDSource;
        private String BaseCcyDeliveryLocationType;
        private String CounterCcyDeliveryLocationType;
        private char RequestFixingInfo;
        private char Side;
        private String TransactTime;
        private double OrderQty;
        private double OrderQty2;
        private char OrdType;
        private double Price;
        private double Price2;
        private String Currency;
        private String QuoteID;
        private char TimeInForce;
        private String ExecInst;
        private String USIIssuerId;
        private String USITradeId;
        private String FarUSIIssuerId;
        private String FarUSITradeId;
        private int LimitPxType;
        private int NoAllocs;
        private String AllocAccount;
        private double AllocQty;
        private String AllocId;
        private double AllocQty2;
        
		public char getOrdType() {
			return OrdType;
		}
		public void setOrdType(char ordType) {
			OrdType = ordType;
		}
		public String getClOrdID() {
			return ClOrdID;
		}
		public void setClOrdID(String clOrdID) {
			ClOrdID = clOrdID;
		}
		public String getMDReqID() {
			return MDReqID;
		}
		public void setMDReqID(String mDReqID) {
			MDReqID = mDReqID;
		}
		public int getNoPartyIDs() {
			return NoPartyIDs;
		}
		public void setNoPartyIDs(int noPartyIDs) {
			NoPartyIDs = noPartyIDs;
		}
		public String getPartyID() {
			return PartyID;
		}
		public void setPartyID(String partyID) {
			PartyID = partyID;
		}
		public char getPartyIDSource() {
			return PartyIDSource;
		}
		public void setPartyIDSource(char partyIDSource) {
			PartyIDSource = partyIDSource;
		}
		public int getPartyRole() {
			return PartyRole;
		}
		public void setPartyRole(int partyRole) {
			PartyRole = partyRole;
		}
		public String getFutSettDate() {
			return FutSettDate;
		}
		public void setFutSettDate(String futSettDate) {
			FutSettDate = futSettDate;
		}
		public String getFutSettDate2() {
			return FutSettDate2;
		}
		public void setFutSettDate2(String futSettDate2) {
			FutSettDate2 = futSettDate2;
		}
		public char getHandlInst() {
			return HandlInst;
		}
		public void setHandlInst(char handlInst) {
			HandlInst = handlInst;
		}
		public String getSymbol() {
			return Symbol;
		}
		public void setSymbol(String symbol) {
			Symbol = symbol;
		}
		public String getCFICode() {
			return CFICode;
		}
		public void setCFICode(String cFICode) {
			CFICode = cFICode;
		}
		public String getSecurityID() {
			return SecurityID;
		}
		public void setSecurityID(String securityID) {
			SecurityID = securityID;
		}
		public String getSecurityIDSource() {
			return SecurityIDSource;
		}
		public void setSecurityIDSource(String securityIDSource) {
			SecurityIDSource = securityIDSource;
		}
		public String getBaseCcyDeliveryLocationType() {
			return BaseCcyDeliveryLocationType;
		}
		public void setBaseCcyDeliveryLocationType(String baseCcyDeliveryLocationType) {
			BaseCcyDeliveryLocationType = baseCcyDeliveryLocationType;
		}
		public String getCounterCcyDeliveryLocationType() {
			return CounterCcyDeliveryLocationType;
		}
		public void setCounterCcyDeliveryLocationType(String counterCcyDeliveryLocationType) {
			CounterCcyDeliveryLocationType = counterCcyDeliveryLocationType;
		}
		public char getRequestFixingInfo() {
			return RequestFixingInfo;
		}
		public void setRequestFixingInfo(char requestFixingInfo) {
			RequestFixingInfo = requestFixingInfo;
		}
		public char getSide() {
			return Side;
		}
		public void setSide(char side) {
			Side = side;
		}
		public String getTransactTime() {
			return TransactTime;
		}
		public void setTransactTime(String transactTime) {
			TransactTime = transactTime;
		}
		public double getOrderQty() {
			return OrderQty;
		}
		public void setOrderQty(double orderQty) {
			OrderQty = orderQty;
		}
		public double getOrderQty2() {
			return OrderQty2;
		}
		public void setOrderQty2(double orderQty2) {
			OrderQty2 = orderQty2;
		}
		public double getPrice() {
			return Price;
		}
		public void setPrice(double price) {
			Price = price;
		}
		public double getPrice2() {
			return Price2;
		}
		public void setPrice2(double price2) {
			Price2 = price2;
		}
		public String getCurrency() {
			return Currency;
		}
		public void setCurrency(String currency) {
			Currency = currency;
		}
		public String getQuoteID() {
			return QuoteID;
		}
		public void setQuoteID(String quoteID) {
			QuoteID = quoteID;
		}
		public char getTimeInForce() {
			return TimeInForce;
		}
		public void setTimeInForce(char timeInForce) {
			TimeInForce = timeInForce;
		}
		public String getExecInst() {
			return ExecInst;
		}
		public void setExecInst(String execInst) {
			ExecInst = execInst;
		}
		public String getUSIIssuerId() {
			return USIIssuerId;
		}
		public void setUSIIssuerId(String uSIIssuerId) {
			USIIssuerId = uSIIssuerId;
		}
		public String getUSITradeId() {
			return USITradeId;
		}
		public void setUSITradeId(String uSITradeId) {
			USITradeId = uSITradeId;
		}
		public String getFarUSIIssuerId() {
			return FarUSIIssuerId;
		}
		public void setFarUSIIssuerId(String farUSIIssuerId) {
			FarUSIIssuerId = farUSIIssuerId;
		}
		public String getFarUSITradeId() {
			return FarUSITradeId;
		}
		public void setFarUSITradeId(String farUSITradeId) {
			FarUSITradeId = farUSITradeId;
		}
		public int getLimitPxType() {
			return LimitPxType;
		}
		public void setLimitPxType(int limitPxType) {
			LimitPxType = limitPxType;
		}
		public int getNoAllocs() {
			return NoAllocs;
		}
		public void setNoAllocs(int noAllocs) {
			NoAllocs = noAllocs;
		}
		public String getAllocAccount() {
			return AllocAccount;
		}
		public void setAllocAccount(String allocAccount) {
			AllocAccount = allocAccount;
		}
		public double getAllocQty() {
			return AllocQty;
		}
		public void setAllocQty(double allocQty) {
			AllocQty = allocQty;
		}
		public String getAllocId() {
			return AllocId;
		}
		public void setAllocId(String allocId) {
			AllocId = allocId;
		}
		public double getAllocQty2() {
			return AllocQty2;
		}
		public void setAllocQty2(double allocQty2) {
			AllocQty2 = allocQty2;
		}
        
	 }
		public static class StructExecutionReport{
		        
		        private String OrderID;
		        private String ClOrdID;
		        private String ExecID;
		        private char ExecType;
		        private char OrdStatus;
		        private String OrdRejReason;
		        private String FutSettDate;
		        private String FutSettDate2;
		        private String Symbol;
		        private String CFICode;
		        private String SecurityID;
		        private String SecurityIDSource;
		        private String BaseCcyDeliveryLocationType;
		        private String CounterCcyDeliveryLocationType;
		        private String MaturityDate;
		        private String MaturityDate2;
		        private String FixingReference;
		        private char Side;
		        private double OrderQty;
		        private double OrderQty2;
		        private char OrdType;
		        private double Price;
		        private double Price2;
		        private String Currency;
		        private char TimeInForce;
		        private double LastQty;
		        private double LastPx;
		        private double LastSpotRate;
		        private double LastForwardPoints;
		        private double LastForwardPoints2;
		        private double LeavesQty;
		        private double CumQty;
		        private String USIIssuerId;
		        private String USITradeId;
		        private String FarUSIIssuerId;
		        private String FarUSITradeId;
		        private double AvgPx;
		        private String Text;
		        private int NoExecs;
		        private String AllocAccount;
		        private String AllocId;
				public String getOrderID() {
					return OrderID;
				}
				public void setOrderID(String orderID) {
					OrderID = orderID;
				}
				public String getClOrdID() {
					return ClOrdID;
				}
				public void setClOrdID(String clOrdID) {
					ClOrdID = clOrdID;
				}
				public String getExecID() {
					return ExecID;
				}
				public void setExecID(String execID) {
					ExecID = execID;
				}
				public char getExecType() {
					return ExecType;
				}
				public void setExecType(char execType) {
					ExecType = execType;
				}
				public char getOrdStatus() {
					return OrdStatus;
				}
				public void setOrdStatus(char ordStatus) {
					OrdStatus = ordStatus;
				}
				public String getOrdRejReason() {
					return OrdRejReason;
				}
				public void setOrdRejReason(String ordRejReason) {
					OrdRejReason = ordRejReason;
				}
				public String getFutSettDate() {
					return FutSettDate;
				}
				public void setFutSettDate(String futSettDate) {
					FutSettDate = futSettDate;
				}
				public String getFutSettDate2() {
					return FutSettDate2;
				}
				public void setFutSettDate2(String futSettDate2) {
					FutSettDate2 = futSettDate2;
				}
				public String getSymbol() {
					return Symbol;
				}
				public void setSymbol(String symbol) {
					Symbol = symbol;
				}
				public String getCFICode() {
					return CFICode;
				}
				public void setCFICode(String cFICode) {
					CFICode = cFICode;
				}
				public String getSecurityID() {
					return SecurityID;
				}
				public void setSecurityID(String securityID) {
					SecurityID = securityID;
				}
				public String getSecurityIDSource() {
					return SecurityIDSource;
				}
				public void setSecurityIDSource(String securityIDSource) {
					SecurityIDSource = securityIDSource;
				}
				public String getBaseCcyDeliveryLocationType() {
					return BaseCcyDeliveryLocationType;
				}
				public void setBaseCcyDeliveryLocationType(String baseCcyDeliveryLocationType) {
					BaseCcyDeliveryLocationType = baseCcyDeliveryLocationType;
				}
				public String getCounterCcyDeliveryLocationType() {
					return CounterCcyDeliveryLocationType;
				}
				public void setCounterCcyDeliveryLocationType(String counterCcyDeliveryLocationType) {
					CounterCcyDeliveryLocationType = counterCcyDeliveryLocationType;
				}
				public String getMaturityDate() {
					return MaturityDate;
				}
				public void setMaturityDate(String maturityDate) {
					MaturityDate = maturityDate;
				}
				public String getMaturityDate2() {
					return MaturityDate2;
				}
				public void setMaturityDate2(String maturityDate2) {
					MaturityDate2 = maturityDate2;
				}
				public String getFixingReference() {
					return FixingReference;
				}
				public void setFixingReference(String fixingReference) {
					FixingReference = fixingReference;
				}
				public char getSide() {
					return Side;
				}
				public void setSide(char side) {
					Side = side;
				}
				public double getOrderQty() {
					return OrderQty;
				}
				public void setOrderQty(double orderQty) {
					OrderQty = orderQty;
				}
				public double getOrderQty2() {
					return OrderQty2;
				}
				public void setOrderQty2(double orderQty2) {
					OrderQty2 = orderQty2;
				}
				public char getOrdType() {
					return OrdType;
				}
				public void setOrdType(char ordType) {
					OrdType = ordType;
				}
				public double getPrice() {
					return Price;
				}
				public void setPrice(double price) {
					Price = price;
				}
				public double getPrice2() {
					return Price2;
				}
				public void setPrice2(double price2) {
					Price2 = price2;
				}
				public String getCurrency() {
					return Currency;
				}
				public void setCurrency(String currency) {
					Currency = currency;
				}
				public char getTimeInForce() {
					return TimeInForce;
				}
				public void setTimeInForce(char timeInForce) {
					TimeInForce = timeInForce;
				}
				public double getLastQty() {
					return LastQty;
				}
				public void setLastQty(double lastQty) {
					LastQty = lastQty;
				}
				public double getLastPx() {
					return LastPx;
				}
				public void setLastPx(double lastPx) {
					LastPx = lastPx;
				}
				public double getLastSpotRate() {
					return LastSpotRate;
				}
				public void setLastSpotRate(double lastSpotRate) {
					LastSpotRate = lastSpotRate;
				}
				public double getLastForwardPoints() {
					return LastForwardPoints;
				}
				public void setLastForwardPoints(double lastForwardPoints) {
					LastForwardPoints = lastForwardPoints;
				}
				public double getLastForwardPoints2() {
					return LastForwardPoints2;
				}
				public void setLastForwardPoints2(double lastForwardPoints2) {
					LastForwardPoints2 = lastForwardPoints2;
				}
				public double getLeavesQty() {
					return LeavesQty;
				}
				public void setLeavesQty(double leavesQty) {
					LeavesQty = leavesQty;
				}
				public double getCumQty() {
					return CumQty;
				}
				public void setCumQty(double cumQty) {
					CumQty = cumQty;
				}
				public String getUSIIssuerId() {
					return USIIssuerId;
				}
				public void setUSIIssuerId(String uSIIssuerId) {
					USIIssuerId = uSIIssuerId;
				}
				public String getUSITradeId() {
					return USITradeId;
				}
				public void setUSITradeId(String uSITradeId) {
					USITradeId = uSITradeId;
				}
				public String getFarUSIIssuerId() {
					return FarUSIIssuerId;
				}
				public void setFarUSIIssuerId(String farUSIIssuerId) {
					FarUSIIssuerId = farUSIIssuerId;
				}
				public String getFarUSITradeId() {
					return FarUSITradeId;
				}
				public void setFarUSITradeId(String farUSITradeId) {
					FarUSITradeId = farUSITradeId;
				}
				public double getAvgPx() {
					return AvgPx;
				}
				public void setAvgPx(double avgPx) {
					AvgPx = avgPx;
				}
				public String getText() {
					return Text;
				}
				public void setText(String text) {
					Text = text;
				}
				public int getNoExecs() {
					return NoExecs;
				}
				public void setNoExecs(int noExecs) {
					NoExecs = noExecs;
				}
				public String getAllocAccount() {
					return AllocAccount;
				}
				public void setAllocAccount(String allocAccount) {
					AllocAccount = allocAccount;
				}
				public String getAllocId() {
					return AllocId;
				}
				public void setAllocId(String allocId) {
					AllocId = allocId;
				}
		        
		      		        
		  }
		public static class StructQuoteCancel{
			
			private String QuoteReqID;
			private int QuoteCancelType;
			private String Symbol;
			private String BaseCcyDeliveryLocationType;
	        private String CounterCcyDeliveryLocationType;
			public String getQuoteReqID() {
				return QuoteReqID;
			}
			public void setQuoteReqID(String quoteReqID) {
				QuoteReqID = quoteReqID;
			}
			public int getQuoteCancelType() {
				return QuoteCancelType;
			}
			public void setQuoteCancelType(int quoteCancelType) {
				QuoteCancelType = quoteCancelType;
			}
			public String getSymbol() {
				return Symbol;
			}
			public void setSymbol(String symbol) {
				Symbol = symbol;
			}
			public String getBaseCcyDeliveryLocationType() {
				return BaseCcyDeliveryLocationType;
			}
			public void setBaseCcyDeliveryLocationType(String baseCcyDeliveryLocationType) {
				BaseCcyDeliveryLocationType = baseCcyDeliveryLocationType;
			}
			public String getCounterCcyDeliveryLocationType() {
				return CounterCcyDeliveryLocationType;
			}
			public void setCounterCcyDeliveryLocationType(String counterCcyDeliveryLocationType) {
				CounterCcyDeliveryLocationType = counterCcyDeliveryLocationType;
			}
	        
		}
		public static class StructDontKnowTrade {
	        private String OrderID;
	        private String ExecID;
	        private char DKReason;
	        private String Symbol;
	        private int Side;
	        private double OrderQty;
	        private String text;
	        
			public String getOrderID() {
				return OrderID;
			}
			public void setOrderID(String orderID) {
				OrderID = orderID;
			}
			public String getExecID() {
				return ExecID;
			}
			public void setExecID(String execID) {
				ExecID = execID;
			}
			public char getDKReason() {
				return DKReason;
			}
			public void setDKReason(char dKReason) {
				DKReason = dKReason;
			}
			public String getSymbol() {
				return Symbol;
			}
			public void setSymbol(String symbol) {
				Symbol = symbol;
			}
			public int getSide() {
				return Side;
			}
			public void setSide(int side) {
				Side = side;
			}
			public double getOrderQty() {
				return OrderQty;
			}
			public void setOrderQty(double d) {
				OrderQty = d;
			}
			public String getText() {
				return text;
			}
			public void setText(String text) {
				this.text = text;
			}
	        
		}
		  public static class StructMassQuoteAck {
		        private String quoteReqID;
		        private String quoteAckStatus;
		        private String tradingSessionId;
		        private String quoteRejectReason;
		        private String text;

		        public void setQuoteReqID(String quoteReqID) {
		            this.quoteReqID = quoteReqID;
		        }

		        public String getQuoteReqID() {
		            return quoteReqID;
		        }

		        public void setQuoteAckStatus(String quoteAckStatus) {
		            this.quoteAckStatus = quoteAckStatus;
		        }

		        public String getQuoteAckStatus() {
		            return quoteAckStatus;
		        }

		        public void setTradingSessionId(String tradingSessionId) {
		            this.tradingSessionId = tradingSessionId;
		        }

		        public String getTradingSessionId() {
		            return tradingSessionId;
		        }

		        public void setQuoteRejectReason(String quoteRejectReason) {
		            this.quoteRejectReason = quoteRejectReason;
		        }

		        public String getQuoteRejectReason() {
		            return quoteRejectReason;
		        }

		        public void setText(String text) {
		            this.text = text;
		        }

		        public String getText() {
		            return text;
		        }
		    }

}
