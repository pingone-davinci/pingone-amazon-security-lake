const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });


const convertToOCSF = async (pingOneEvents) => {
    
    const ocsfEvents = [];
    
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
              "message": pingOneEvent?.action?.type, 
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

            ocsfEvents.push(ocsfEvent);

        } else if(pingOneEvent?.action?.type === 'PASSWORD.RESET' ||
                  pingOneEvent?.action?.type === 'PASSWORD.RECOVERY') {
          
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
              "activity_id": 4,
              "activity_name": "Password Reset",
              "category_name": "Audit Activity",
              "category_uid": 3,
              "class_name": "Account Change",
              "class_uid": 3001,
              "data": {
                "environment": pingOneEvent.resources[0].environment.id
              },
              "message": pingOneEvent?.action?.type, 
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
              "type_name": "Account Change: Password Reset",  
              "type_uid": 300104,
              "user": {
                "name": pingOneEvent.resources[0].name, // resources[0].name
                "uid": pingOneEvent.resources[0].id, // resources[0].id
                "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
              }
            } 

            ocsfEvents.push(ocsfEvent);
          
        } else if(pingOneEvent?.action?.type === 'USER.UPDATED')  {
          
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
              "activity_id": 7,
              "activity_name": "Change",
              "category_name": "Audit Activity",
              "category_uid": 3,
              "class_name": "Account Change",
              "class_uid": 3001,
              "data": {
                "environment": pingOneEvent.resources[0].environment.id
              },
              "message": pingOneEvent?.action?.type, 
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
              "type_name": "Account Change: Change",  
              "type_uid": 300107,
              "user": {
                "name": pingOneEvent.resources[0].name, // resources[0].name
                "uid": pingOneEvent.resources[0].id, // resources[0].id
                "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
              }
            } 

            ocsfEvents.push(ocsfEvent);
          
        } else if(pingOneEvent?.action?.type === 'USER.CREATED')  {
          
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
              "activity_name": "Create",
              "category_name": "Audit Activity",
              "category_uid": 3,
              "class_name": "Account Change",
              "class_uid": 3001,
              "data": {
                "environment": pingOneEvent.resources[0].environment.id
              },
              "message": pingOneEvent?.action?.type, 
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
              "type_name": "Account Change: Create",  
              "type_uid": 300101,
              "user": {
                "name": pingOneEvent.resources[0].name, // resources[0].name
                "uid": pingOneEvent.resources[0].id, // resources[0].id
                "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
              }
            } 

            ocsfEvents.push(ocsfEvent);
          
        } else if(pingOneEvent?.action?.type === 'USER.DELETED')  {
          
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
              "activity_id": 6,
              "activity_name": "Delete",
              "category_name": "Audit Activity",
              "category_uid": 3,
              "class_name": "Account Change",
              "class_uid": 3001,
              "data": {
                "environment": pingOneEvent.resources[0].environment.id
              },
              "message": pingOneEvent?.action?.type, 
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
              "type_name": "Account Change: Delete",  
              "type_uid": 300106,
              "user": {
                "name": pingOneEvent.resources[0].name, // resources[0].name
                "uid": pingOneEvent.resources[0].id, // resources[0].id
                "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
              }
            } 

            ocsfEvents.push(ocsfEvent);
          
        } else {
            console.log(`OCSF event conversion for event type "${pingOneEvent?.action?.type}" not implemented, skipping event.`)            
        }
    }
    
    return ocsfEvents;
}


exports.handler = async (event) => {
   let pingOneEvent; 
   
    // For testing purposes
    if(event.body) {
        pingOneEvent = JSON.parse(event.body);
    } else {
        pingOneEvent = event;
    }
    
    console.log(`pingOneEvent: ${JSON.stringify(pingOneEvent)}`);

    try {
        const ocsfEvents = await convertToOCSF(pingOneEvent);
        
        console.log(ocsfEvents)

        for(const ocsfEvent of ocsfEvents) {
            
            const params = {
                Bucket:  process.env.S3_BUCKET,
                Key: ocsfEvent.metadata.uid,
                Body: JSON.stringify(ocsfEvent),
            };

            const response = await s3.upload(params).promise();
        }
        
        // Success
        return {statusCode: 200};

    } catch (err) {
        console.log(err);
        
        const error = {
            err_msg: err
        }
        
        return {
            statusCode: 500, 
            body: JSON.stringify(error)
        };
    }
};
