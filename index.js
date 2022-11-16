const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });


const convertToOCSF = async (pingOneEvents) => {
    
    for(var i = 0; i < pingOneEvents.length; i++) {
    
        const pingOneEvent = pingOneEvents[i];
        
        // Authentication event
        if(pingOneEvent?.action?.type === 'FLOW.DELETED' || 
           pingOneEvent?.action?.type === 'FLOW.UPDATED') {
            
            let statusId = 0;
            
            if(pingOneEvent.result.status === 'SUCCESS') {
                statusId = 1;
            } else if(pingOneEvent.result.status === 'FAILED') {
                statusId = 2;
            } else {
                statusId = -1;
            }
            
            const isoStr = pingOneEvent.recordedAt;
            const date = new Date(isoStr);
            const timestamp = date.getTime();    
            const timezone_offset = date.getTimezoneOffset();

            const ocsfEvent = {
              "activity_id": 1,
              "activity_name": "Flow Completed",
              "auth_protocol": "Unknown",
              "auth_protocol_id": 0,
              "category_name": "Audit Activity",
              "category_uid": 3,
              "class_name": "Authentication",
              "class_uid": 3002,
              "data": {
                "environment": pingOneEvent.actors.user.environment.id
              },
              "dst_endpoint": {
                "svc_name": pingOneEvent.actors.client.name  // Region specifc ?
              },
              "logon_type": "Network",
              "logon_type_id": 3,
              "message": "Flow Completed", // Flow Completed
              "metadata": {
                "correlation_uid": pingOneEvent.resources[0].id,
                "uid": pingOneEvent.id, // id
                "original_time": pingOneEvent.recordedAt, // recordedAt
                "product": {
                  "lang": "en",
                  "name": "PingOne",
                  "vendor_name": "Ping Identity"
                },
                "version": "1.0.0"
              },
              "severity": "Informational", 
              "severity_id": 1,
              "status": pingOneEvent.result.status, // result
              "status_detail": pingOneEvent.result.description, // result.description
              "status_id": statusId, // 1 for success, 2 for failure, 0 for unknown
              "time": timestamp, 
              "timezone_offset": timezone_offset, 
              "type_name": "Authentication: Logon",  
              "type_uid": 300201,
              "user": {
                "name": pingOneEvent.actors.user.name, // user.name
                "uid": pingOneEvent.actors.user.id, // user.id
                "org_uid": pingOneEvent.actors.user.environment.id // user.environment.id
              }
            } 

            return ocsfEvent;
        }
    }   
    
    return 'Failed produce OCSF event';
}


exports.handler = async (event) => {

    const ocsfEvent = await convertToOCSF(event);
    
    const params = {
        Bucket:  process.env.S3_BUCKET,
        Key: ocsfEvent.metadata.uid,
        Body: JSON.stringify(ocsfEvent),
    };

    try {
        const response = await s3.upload(params).promise();
        return response;

    } catch (err) {
        console.log(err);
    }
};

