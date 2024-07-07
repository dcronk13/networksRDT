from segment import Segment
from operator import attrgetter


# #################################################################################################################### #
# RDTLayer                                                                                                             #
#                                                                                                                      #
# Description:                                                                                                         #
# The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable         #
# channel.                                                                                                             #
#                                                                                                                      #
#                                                                                                                      #
# Notes:                                                                                                               #
# This file is meant to be changed.                                                                                    #
#                                                                                                                      #
#                                                                                                                      #
# #################################################################################################################### #


class RDTLayer(object):
    # ################################################################################################################ #
    # Class Scope Variables                                                                                            #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    DATA_LENGTH = 4 # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # Receive window size for flow-control
    sendChannel = None
    receiveChannel = None
    dataToSend = ''
    currentIteration = 0                                # Use this for segment 'timeouts'
    #Stores seqnum and payload from client sent segments
    segmentStoreServer = []
    #Stores acknums from server sent segments
    segmentStoreClient = []
    #Stores seqnum and payload in client itself to check against storeClient
    segmentSent = []
    #Counter for timeouts
    countSegmentTimeouts = 0
    #NAK is used to track error packets. Current checksum doesn't work fully, but has about 3/4 success in small text
    NAK = []
    # Add items as needed

    # ################################################################################################################ #
    # __init__()                                                                                                       #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        # Add items as needed

    # ################################################################################################################ #
    # setSendChannel()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable sending lower-layer channel                                                 #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setSendChannel(self, channel):
        self.sendChannel = channel

    # ################################################################################################################ #
    # setReceiveChannel()                                                                                              #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable receiving lower-layer channel                                               #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setReceiveChannel(self, channel):
        self.receiveChannel = channel

    # ################################################################################################################ #
    # setDataToSend()                                                                                                  #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the string data to send                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setDataToSend(self,data):
        self.dataToSend = data

    # ################################################################################################################ #
    # getDataReceived()                                                                                                #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to get the currently received and buffered string data, in order                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def getDataReceived(self):
        # ############################################################################################################ #
        # Identify the data that has been received...
        #Sets empty string variable
        msg = ''
        #Goes through what the server has stored from client, concatenates the payload of each segment
        #Since they are ordered, this will return the correct information
        for x in self.segmentStoreServer:
            msg += x.payload

        # ############################################################################################################ #
        return msg

    # ################################################################################################################ #
    # processData()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # "timeslice". Called by main once per iteration                                                                   #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processData(self):
        self.currentIteration += 1
        self.processSend()
        self.processReceiveAndSendRespond()

    # ################################################################################################################ #
    # processSend()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment sending tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processSend(self):
        #If there is data to send, this is a client call
        if self.dataToSend != '':
            #For x from 0-15, using the window size and data length to set range
            for x in range(0,self.FLOW_CONTROL_WIN_SIZE-self.DATA_LENGTH,self.DATA_LENGTH):

                #The actual algorithm to get the correct spot took a little bit of playing around
                #Essentially, any array spot can be reached by taking the current iteration (starting from 1), taking away 1 (so we can reach 0)
                #mulltiplying by 12 - since different send windows are 12 seqnums apart. Finally, you take this value, add the current loop counter
                # seqnum 0: 12*(1-1) + 0 = 0
                #seqnum 16: 12*(2-1) + 4 = 16

                #Here we check if the information to be sent is out of the range of the data to send AKA all data has been sent
                #Checks if index spot at seqnum is larger than indexes in dataToSend
                if (12*(self.currentIteration-1)+x+4) > len(self.dataToSend):
                    return

                #If statement to see if this segment will be within the dataToSend by seeing if the index spot + 4 (end of that segment) is within
                #dataToSend
                if ((12*(self.currentIteration-1)+x+4) <= len(self.dataToSend)):
                    #Create segment to send
                    segmentSend = Segment()
                    #Set seqnum using algorithm mention above, then set data by index slicing dataToSend, with the first element at seqnum, and last
                    #element at seqnum+4 for 4 characters
                    segmentSend.setData(12*(self.currentIteration-1)+x,self.dataToSend[12*(self.currentIteration-1)+x:(12*(self.currentIteration-1))+x+4])

                    #Set start iteration and startdelay for checking delayed packets
                    segmentSend.setStartIteration(self.currentIteration)
                    segmentSend.setStartDelayIteration(self.currentIteration+3)
                    #Put segment in list of sent segment for checking later
                    self.segmentSent.append(segmentSend)
                    #Send Segment
                    self.sendChannel.send(segmentSend)
                    print("Sending segment: ", segmentSend.to_string()) 
                else:
                    #If the previous statement was false, this will be last statement. Do the same process but when indexing, make final index the last 
                    #element of dataToSend
                    segmentSend = Segment()
                    segmentSend.setData(12*(self.currentIteration-1)+x,self.dataToSend[12*(self.currentIteration-1)+x:len(self.dataToSend)-1])
                    segmentSend.setStartIteration(self.currentIteration)
                    segmentSend.setStartDelayIteration(self.currentIteration+3)
                    self.segmentSent.append(segmentSend)
                    self.sendChannel.send(segmentSend)
                    print("Sending segment: ", segmentSend.to_string())
                    #Note the break is here to exit so next time around no segments will be sent
                    break
        

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processReceiveAndSendRespond(self):
        # This call returns a list of incoming segments (see Segment class)...
        listIncomingSegments = self.receiveChannel.receive()
    
        # ############################################################################################################ #
        # What segments have been received?
        # How will you get them back in order?
        # This is where a majority of your logic will be implemented
    
        #If no data to send, we're server side
        if self.dataToSend == '':
            #For segments in incoming Segments...
            for i in listIncomingSegments:
                #If non negative seqnum, we are dealing with a data segment
                if i.seqnum != -1:
                    #If segment fails checksum
                    if i.checksum != i.calc_checksum(i.to_string()):
                        #Check if this seqnum is not in NAK
                        if i.seqnum not in self.NAK:
                            #If it isn't put in NAK array
                            self.NAK.append(i.seqnum)
                        #Remove segment from incoming so the server doesn't store bad info
                        listIncomingSegments.remove(i)
                        continue
                    #If the seqnum is in NAK, means client retransmitted, passed checksum, and is now good to go
                    if i.seqnum in self.NAK:
                        #As such, remove from NAK array
                        self.NAK.remove(i.seqnum)
                    #If segment is already stored, means server sent ack, but client did not recieve. Resend ack immediately
                    if i in self.segmentStoreServer:
                        #Create new ack                      
                        segmentAck = Segment()
                        #Set acknum to seqnum+4
                        segmentAck.setAck(i.seqnum+4)
                        print("Retransmitting ack: ",segmentAck.to_string())
                        #Send ack and remove from incoming so server doesn't duplicate data
                        self.sendChannel.send(segmentAck)
                        listIncomingSegments.remove(i)
                        continue
            #Check data used to see if there are duplicates in server data
            checkData = 0
            #For segments incoming
            for i in listIncomingSegments:
                #For segments already stored
                for j in self.segmentStoreServer:
                    #If there are duplicate values, flag
                    if i.seqnum == j.seqnum:
                        checkData == 1
                #if no flags
                if checkData == 0:
                    #Put segment in server storage
                    self.segmentStoreServer.append(i)
                #Reset flag for next iteration
                checkData = 0
            #Used another duplicate check. Had a lot of issues with this but after this segment, no duplicates were found
            #Prev holds seqnum of previous segment
            prev = -1
            #For segments in storage
            for i in self.segmentStoreServer:
                #If first loop ie seqnum = 0
                if prev == -1:
                    #set prev to 0
                    prev = i.seqnum
                    continue
                #If prior seqnum is equal to current, its a duplicate
                if i.seqnum == prev:
                    #Get rid of duplicate
                    self.segmentStoreServer.remove(i)
                    continue
                #Set previous to current (Only happens if no duplicates found)
                prev = i.seqnum
            #Sort server storage
            self.segmentStoreServer.sort(key = attrgetter('seqnum'))
            #Check NAK is a flag used to see if a segment in NAK needs to be retransmitted
            checkNAK = 0
            #For values in NAK
            for i in self.NAK:
                #For segments in server store
                for j in self.segmentStoreServer:
                    #If there are matching seq nums, NAK has already been fixed
                    if i == j.seqnum:
                        checkNAK = 1
                #If no flag went up, needs to be retransmitted
                if checkNAK == 0:
                    #Create new ack segment to ask client for error free segment
                    segmentNAK = Segment()
                    #USed negative values as a flag for ack num. This is so I could reverse the operation on either
                    #side and know exactly what packet needs to be sent/received
                    segmentNAK.setAck(-1*(i+4))
                    print("(ERROR) Retransmitting ack: ",segmentNAK.to_string())
                    self.sendChannel.send(segmentNAK)
                checkNAK = 0
        #Else, we are dealing with client side. Client side accepts acknowledgements and retransmits if there are errors
        else:
            #For segments incoming (all ACK segments)
            for i in listIncomingSegments:
                #If ack less than 0, we have a error segment ack
                if i.acknum < 0:
                    #Get resend seqnum
                    resend = (i.acknum * -1) - 4
                    #For segments in sent
                    for j in self.segmentSent:
                        #if this resend seqnum equals a seqnum in sent, replace with new
                        if resend == j.seqnum:
                            #Create new segment as we did in processSend
                            newSeg = Segment()
                            newSeg.setData(j.seqnum,self.dataToSend[j.seqnum:j.seqnum+4])
                            print("(Error) Retransmitting segment:",newSeg.to_string())
                            newSeg.setStartIteration(self.currentIteration)
                            newSeg.setStartDelayIteration(self.currentIteration+3)
                            #Replaces current segment in segment sent store with new segment seqnum/4 gets index
                            self.segmentSent[int(newSeg.seqnum/4)] = newSeg
                            self.sendChannel.send(newSeg)
                #Else if the segment in the incoming Segments has been retrieved already, (duplicate ack), delete ack from segments incoming
                elif i in self.segmentStoreClient:
                    listIncomingSegments.delete(i)
            #Take incoming segments and append. Don't need to check errors since they only apply to server receiving data segments
            for i in listIncomingSegments:
                self.segmentStoreClient.append(i)
            self.segmentStoreClient = list(set(self.segmentStoreClient))
            self.segmentStoreClient.sort(key = attrgetter('acknum'))


        # ############################################################################################################ #
        # How do you respond to what you have received?
        # How can you tell data segments apart from ack segemnts?
        
        #This section used by server to send acks and clients to retransmit segments

        #If data to send is empty, server side
        if self.dataToSend == '':
            #For segments in incoming
             for i in listIncomingSegments:
                 #If seqnum of segment is not -1, we have a data segment. Send an ack to client
                 if (i.seqnum != -1):
                    #Create ack segment, set acknum to seqnum +4 to tell client it is caught up to that                     
                    segmentAck = Segment()
                    segmentAck.setAck(i.seqnum+4)
                    print("Sending ack: ", segmentAck.to_string())
                    self.sendChannel.send(segmentAck)

        #Client side since dataTosend is not empty
        else:
            #Only used for first iteration, segment store client is not filled until servers first iteration, and this runs on clients first
            if len(self.segmentStoreClient) == 0:
                return
            #retrans is a flag for if client needs to retransmit
            retrans = 1
            #Expected tells client how much data is expected for server to have
            expected = (self.currentIteration-1) * 3
            #Keeps track of segments that need to be retransmitted
            retransList = []
            #From 0 to expected data, incrementing by 4
            for i in range(0,expected*self.DATA_LENGTH,self.DATA_LENGTH):
                #For segments in client storage
                for j in self.segmentStoreClient:
                    #if there is a seqnum for ack, no need to retrans - turn off flag
                    if i == j.acknum - 4:
                        retrans = 0
                #If retrans flag on, append to retrans list
                if retrans == 1:
                    retransList.append(i)
                retrans = 1
            #For retrans list
            for i in retransList:
                #for segments in segmentSent
                for j in self.segmentSent:
                    #if seqnum in retrans list matches sent, need to send new. But this only applies if the segment already sent is not timedout
                    if i == j.seqnum and j.getStartDelayIteration() <= self.currentIteration:
                        #If timedout, increment
                        self.countSegmentTimeouts += 1
                        #Create new segment
                        newSeg = Segment()
                        newSeg.setData(j.seqnum,self.dataToSend[j.seqnum:j.seqnum+4])
                        print("(Segment Timeout) Retransmitting segment: ", newSeg.to_string())
                        newSeg.setStartIteration(self.currentIteration)
                        newSeg.setStartDelayIteration(self.currentIteration+3)
                        #Put new created segment in segmentSent
                        self.segmentSent[int(newSeg.seqnum/4)] = newSeg
                        self.sendChannel.send(newSeg)



                

