const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });
const parquet = require('parquetjs');
const fs = require('fs');

const convertToOCSF = async (pingOneEvents) => {
  var fiveMins = 1000 * 60 * 5;
  var date = new Date();  //or use any other date
  var roundedDate = new Date(Math.round(date.getTime() / fiveMins) * fiveMins);

  const eventHour = getEventHour(new Date());

  for (var i = 0; i < pingOneEvents.length; i++) {

    const pingOneEvent = pingOneEvents[i];

    let ocsfEvent; 
    let schemaEvent;
    let event_type_uid;

    console.log(`Processing PingOne event type: ${pingOneEvent?.action?.type}`);

    /*
      Authentication success event 
    */
    if (pingOneEvent?.action?.type === 'FLOW.DELETED') {

      let statusId = 0;

      if (pingOneEvent.result.status === 'SUCCESS') {
        statusId = 1;
      } else if (pingOneEvent.result.status === 'FAILED') {
        statusId = 2;
      }

      const isoStr = pingOneEvent.recordedAt;
      const date = new Date(isoStr);
      const timestamp = date.getTime();
      const timezone_offset = date.getTimezoneOffset();

      event_type_uid = 300201;

      ocsfEvent = {
        "activity_id": 1,
        "auth_protocol_id": 0,
        "category_name": "Audit Activity",
        "category_uid": 3,
        "class_name": "Authentication",
        "class_uid": 3002,
        "actor": {
          "idp": {
            "name": "PingOne",
            "uid": pingOneEvent.resources[0].environment.id
          },
          "user":  {
            "name": pingOneEvent.resources[0].name, // resources[0].name
            "uid": pingOneEvent.resources[0].id, // resources[0].id
          } 
        },        
        "dst_endpoint": {
          "svc_name": pingOneEvent.actors.client.name
        },
        "logon_type": "Network",
        "logon_type_id": 99,
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
        "type_uid": event_type_uid,
        "user": {
          "name": pingOneEvent.actors.user.name, // user.name
          "uid": pingOneEvent.actors.user.id, // user.id
          "org_uid": pingOneEvent.actors.user.environment.id // user.environment.id
        }
      }

      schemaEvent = new parquet.ParquetSchema({
        activity_id: { type: 'INT32' },
        auth_protocol_id: { type: 'INT32' },
        category_name: { type: 'UTF8' },
        category_uid: { type: 'INT32' },
        class_name: { type: 'UTF8' },
        class_uid: { type: 'INT32' },
        actor: {
          repeated: false,
          fields: {
            idp: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }, 
            user: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }
          }
        },
        dst_endpoint: {
          repeated: false,
          fields: {
            svc_name: { type: 'UTF8' },
          },
        },
        logon_type: { type: 'UTF8' },
        logon_type_id: { type: 'INT32' },
        message: { type: 'UTF8' },
        metadata: {
          repeated: false,
          fields: {
            correlation_uid: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            original_time: { type: 'UTF8' },
            product: {
              repeated: false,
              fields: {
                lang: { type: 'UTF8' },
                name: { type: 'UTF8' },
                vendor_name: { type: 'UTF8' },
              }
            },
            version: { type: 'UTF8' }
          }
        },
        severity: { type: 'UTF8' },
        severity_id: { type: 'INT32' },
        status: { type: 'UTF8' },
        status_detail: { type: 'UTF8' },
        status_id: { type: 'INT32' },
        time: { type: 'INT64' },
        timezone_offset: { type: 'INT32' },
        type_name: { type: 'UTF8' },
        type_uid: { type: 'INT32' },
        user: {
          repeated: false,
          fields: {
            name: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            org_uid: { type: 'UTF8' },
          }
        },
      });



      /*
        Authentication failure event 
      */
    } else if (pingOneEvent?.action?.type === 'FLOW.UPDATED') {

        if (pingOneEvent.result.status === 'FAILED') {
          statusId = 2;
        } else {
          // Skip FLOW.UPDATED success events as they do not mean an authentication failure
          continue;
        }
  
        const isoStr = pingOneEvent.recordedAt;
        const date = new Date(isoStr);
        const timestamp = date.getTime();
        const timezone_offset = date.getTimezoneOffset();
  
        event_type_uid = 300201;
  
        ocsfEvent = {
          "activity_id": 1,
          "auth_protocol_id": 0,
          "category_name": "Audit Activity",
          "category_uid": 3,
          "class_name": "Authentication",
          "class_uid": 3002,
          "actor": {
            "idp": {
              "name": "PingOne",
              "uid": pingOneEvent.resources[0].environment.id
            },
            "user":  {
              "name": pingOneEvent.resources[0].name, // resources[0].name
              "uid": pingOneEvent.resources[0].id, // resources[0].id
            } 
          },        
          "dst_endpoint": {
            "svc_name": pingOneEvent.actors.client.name
          },
          "logon_type": "Network",
          "logon_type_id": 99,
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
          "type_uid": event_type_uid,
          "user": {
            "name": pingOneEvent.resources[0].name, // user.name
            "uid": pingOneEvent.resources[0].id, // user.id
            "org_uid": pingOneEvent.resources[0].environment.id // user.environment.id
          }
        }
  
        schemaEvent = new parquet.ParquetSchema({
          activity_id: { type: 'INT32' },
          auth_protocol_id: { type: 'INT32' },
          category_name: { type: 'UTF8' },
          category_uid: { type: 'INT32' },
          class_name: { type: 'UTF8' },
          class_uid: { type: 'INT32' },
          actor: {
            repeated: false,
            fields: {
              idp: {
                repeated: false,
                fields: {
                  name: { type: 'UTF8' },
                  uid: { type: 'UTF8' },
                }
              }, 
              user: {
                repeated: false,
                fields: {
                  name: { type: 'UTF8' },
                  uid: { type: 'UTF8' },
                }
              }
            }
          },
          dst_endpoint: {
            repeated: false,
            fields: {
              svc_name: { type: 'UTF8' },
            },
          },
          logon_type: { type: 'UTF8' },
          logon_type_id: { type: 'INT32' },
          message: { type: 'UTF8' },
          metadata: {
            repeated: false,
            fields: {
              correlation_uid: { type: 'UTF8' },
              uid: { type: 'UTF8' },
              original_time: { type: 'UTF8' },
              product: {
                repeated: false,
                fields: {
                  lang: { type: 'UTF8' },
                  name: { type: 'UTF8' },
                  vendor_name: { type: 'UTF8' },
                }
              },
              version: { type: 'UTF8' }
            }
          },
          severity: { type: 'UTF8' },
          severity_id: { type: 'INT32' },
          status: { type: 'UTF8' },
          status_detail: { type: 'UTF8' },
          status_id: { type: 'INT32' },
          time: { type: 'INT64' },
          timezone_offset: { type: 'INT32' },
          type_name: { type: 'UTF8' },
          type_uid: { type: 'INT32' },
          user: {
            repeated: false,
            fields: {
              name: { type: 'UTF8' },
              uid: { type: 'UTF8' },
              org_uid: { type: 'UTF8' },
            }
          },
        });
 
      /*
        Authentication failure event 
      */
      } else if (pingOneEvent?.action?.type === 'PASSWORD.CHECK_FAILED') {
  
        const isoStr = pingOneEvent.recordedAt;
        const date = new Date(isoStr);
        const timestamp = date.getTime();
        const timezone_offset = date.getTimezoneOffset();
  
        event_type_uid = 300201;
  
        ocsfEvent = {
          "activity_id": 1,
          "auth_protocol_id": 0,
          "category_name": "Audit Activity",
          "category_uid": 3,
          "class_name": "Authentication",
          "class_uid": 3002,
          "actor": {
            "idp": {
              "name": "PingOne",
              "uid": pingOneEvent.resources[0].environment.id
            },
            "user":  {
              "name": pingOneEvent.resources[0].name, // resources[0].name
              "uid": pingOneEvent.resources[0].id, // resources[0].id
            } 
          },        
          "dst_endpoint": {
            "svc_name": "PingOne"
          },
          "logon_type": "Network",
          "logon_type_id": 99,
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
          "status": "FAILED", // Result from PASSWORD.CHECK_FAILED is actually SUCCESS 
          "status_detail": pingOneEvent.result.description, // result.description
          "status_id": 2, // 1 for success, 2 for failure, 0 for unknown
          "time": timestamp,
          "timezone_offset": timezone_offset,
          "type_name": "Authentication: Logon",
          "type_uid": event_type_uid,
          "user": {
            "name": pingOneEvent.resources[0].name, // user.name
            "uid": pingOneEvent.resources[0].id, // user.id
            "org_uid": pingOneEvent.resources[0].environment.id // user.environment.id
          }
        }
  
        schemaEvent = new parquet.ParquetSchema({
          activity_id: { type: 'INT32' },
          auth_protocol_id: { type: 'INT32' },
          category_name: { type: 'UTF8' },
          category_uid: { type: 'INT32' },
          class_name: { type: 'UTF8' },
          class_uid: { type: 'INT32' },
          actor: {
            repeated: false,
            fields: {
              idp: {
                repeated: false,
                fields: {
                  name: { type: 'UTF8' },
                  uid: { type: 'UTF8' },
                }
              }, 
              user: {
                repeated: false,
                fields: {
                  name: { type: 'UTF8' },
                  uid: { type: 'UTF8' },
                }
              }
            }
          },
          dst_endpoint: {
            repeated: false,
            fields: {
              svc_name: { type: 'UTF8' },
            },
          },
          logon_type: { type: 'UTF8' },
          logon_type_id: { type: 'INT32' },
          message: { type: 'UTF8' },
          metadata: {
            repeated: false,
            fields: {
              correlation_uid: { type: 'UTF8' },
              uid: { type: 'UTF8' },
              original_time: { type: 'UTF8' },
              product: {
                repeated: false,
                fields: {
                  lang: { type: 'UTF8' },
                  name: { type: 'UTF8' },
                  vendor_name: { type: 'UTF8' },
                }
              },
              version: { type: 'UTF8' }
            }
          },
          severity: { type: 'UTF8' },
          severity_id: { type: 'INT32' },
          status: { type: 'UTF8' },
          status_detail: { type: 'UTF8' },
          status_id: { type: 'INT32' },
          time: { type: 'INT64' },
          timezone_offset: { type: 'INT32' },
          type_name: { type: 'UTF8' },
          type_uid: { type: 'INT32' },
          user: {
            repeated: false,
            fields: {
              name: { type: 'UTF8' },
              uid: { type: 'UTF8' },
              org_uid: { type: 'UTF8' },
            }
          },
        });      
  
  
        /*
          Password Reset & Recovery Event 
        */
      } 
      else if (pingOneEvent?.action?.type === 'PASSWORD.RESET' ||
      pingOneEvent?.action?.type === 'PASSWORD.RECOVERY') {

      let statusId = 0;

      if (pingOneEvent.result.status === 'SUCCESS') {
        statusId = 1;
      } else if (pingOneEvent.result.status === 'FAILED') {
        statusId = 2;
      }

      const isoStr = pingOneEvent.recordedAt;
      const date = new Date(isoStr);
      const timestamp = date.getTime();
      const timezone_offset = date.getTimezoneOffset();

      event_type_uid = 300104;

      ocsfEvent = {
        "activity_id": 4,
        "activity_name": "Password Reset",
        "category_name": "Audit Activity",
        "category_uid": 3,
        "class_name": "Account Change",
        "class_uid": 3001,
        "actor": {
          "idp": {
            "name": "PingOne",
            "uid": pingOneEvent.resources[0].environment.id
          },
          "user":  {
            "name": pingOneEvent.resources[0].name, // resources[0].name
            "uid": pingOneEvent.resources[0].id, // resources[0].id
          } 
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
        "type_uid": event_type_uid,
        "user": {
          "name": pingOneEvent.resources[0].name, // resources[0].name
          "uid": pingOneEvent.resources[0].id, // resources[0].id
          "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
        }
      }

      schemaEvent = new parquet.ParquetSchema({
        activity_id: { type: 'INT32' },
        category_name: { type: 'UTF8' },
        category_uid: { type: 'INT32' },
        class_name: { type: 'UTF8' },
        class_uid: { type: 'INT32' },
        actor: {
          repeated: false,
          fields: {
            idp: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }, 
            user: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }
          }
        },
        message: { type: 'UTF8' },
        metadata: {
          repeated: false,
          fields: {
            correlation_uid: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            original_time: { type: 'UTF8' },
            product: {
              repeated: false,
              fields: {
                lang: { type: 'UTF8' },
                name: { type: 'UTF8' },
                vendor_name: { type: 'UTF8' },
              }
            },
            version: { type: 'UTF8' }
          }
        },
        severity: { type: 'UTF8' },
        severity_id: { type: 'INT32' },
        status: { type: 'UTF8' },
        status_detail: { type: 'UTF8' },
        status_id: { type: 'INT32' },
        time: { type: 'INT64' },
        timezone_offset: { type: 'INT32' },
        type_name: { type: 'UTF8' },
        type_uid: { type: 'INT32' },
        user: {
          repeated: false,
          fields: {
            name: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            org_uid: { type: 'UTF8' },
          }
        },
      });      

      /*
        User Updated Event
      */
    } else if (pingOneEvent?.action?.type === 'USER.UPDATED') {

      let statusId = 0;

      if (pingOneEvent.result.status === 'SUCCESS') {
        statusId = 1;
      } else if (pingOneEvent.result.status === 'FAILED') {
        statusId = 2;
      }

      const isoStr = pingOneEvent.recordedAt;
      const date = new Date(isoStr);
      const timestamp = date.getTime();
      const timezone_offset = date.getTimezoneOffset();

      event_type_uid = 300107;

      ocsfEvent = {
        "activity_id": 7,
        "activity_name": "Change",
        "category_name": "Audit Activity",
        "category_uid": 3,
        "class_name": "Account Change",
        "class_uid": 3001,
        "actor": {
          "idp": {
            "name": "PingOne",
            "uid": pingOneEvent.resources[0].environment.id
          },
          "user":  {
            "name": pingOneEvent.resources[0].name, // resources[0].name
            "uid": pingOneEvent.resources[0].id, // resources[0].id
          } 
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
        "type_uid": event_type_uid,
        "user": {
          "name": pingOneEvent.resources[0].name, // resources[0].name
          "uid": pingOneEvent.resources[0].id, // resources[0].id
          "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
        }
      }

      schemaEvent = new parquet.ParquetSchema({
        activity_id: { type: 'INT32' },
        category_name: { type: 'UTF8' },
        category_uid: { type: 'INT32' },
        class_name: { type: 'UTF8' },
        class_uid: { type: 'INT32' },
        actor: {
          repeated: false,
          fields: {
            idp: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }, 
            user: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }
          }
        },
        message: { type: 'UTF8' },
        metadata: {
          repeated: false,
          fields: {
            correlation_uid: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            original_time: { type: 'UTF8' },
            product: {
              repeated: false,
              fields: {
                lang: { type: 'UTF8' },
                name: { type: 'UTF8' },
                vendor_name: { type: 'UTF8' },
              }
            },
            version: { type: 'UTF8' }
          }
        },
        severity: { type: 'UTF8' },
        severity_id: { type: 'INT32' },
        status: { type: 'UTF8' },
        status_detail: { type: 'UTF8' },
        status_id: { type: 'INT32' },
        time: { type: 'INT64' },
        timezone_offset: { type: 'INT32' },
        type_name: { type: 'UTF8' },
        type_uid: { type: 'INT32' },
        user: {
          repeated: false,
          fields: {
            name: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            org_uid: { type: 'UTF8' },
          }
        },
      });      

      /*
        User Created Event
      */
    } else if (pingOneEvent?.action?.type === 'USER.CREATED') {

      let statusId = 0;

      if (pingOneEvent.result.status === 'SUCCESS') {
        statusId = 1;
      } else if (pingOneEvent.result.status === 'FAILED') {
        statusId = 2;
      }

      const isoStr = pingOneEvent.recordedAt;
      const date = new Date(isoStr);
      const timestamp = date.getTime();
      const timezone_offset = date.getTimezoneOffset();

      event_type_uid = 300101;

      ocsfEvent = {
        "activity_id": 1,
        "activity_name": "Create",
        "category_name": "Audit Activity",
        "category_uid": 3,
        "class_name": "Account Change",
        "class_uid": 3001,
        "actor": {
          "idp": {
            "name": "PingOne",
            "uid": pingOneEvent.resources[0].environment.id
          },
          "user":  {
            "name": pingOneEvent.resources[0].name, // resources[0].name
            "uid": pingOneEvent.resources[0].id, // resources[0].id
          } 
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
        "type_uid": event_type_uid,
        "user": {
          "name": pingOneEvent.resources[0].name, // resources[0].name
          "uid": pingOneEvent.resources[0].id, // resources[0].id
          "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
        }
      }

      schemaEvent = new parquet.ParquetSchema({
        activity_id: { type: 'INT32' },
        category_name: { type: 'UTF8' },
        category_uid: { type: 'INT32' },
        class_name: { type: 'UTF8' },
        class_uid: { type: 'INT32' },
        actor: {
          repeated: false,
          fields: {
            idp: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }, 
            user: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }
          }
        },
        message: { type: 'UTF8' },
        metadata: {
          repeated: false,
          fields: {
            correlation_uid: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            original_time: { type: 'UTF8' },
            product: {
              repeated: false,
              fields: {
                lang: { type: 'UTF8' },
                name: { type: 'UTF8' },
                vendor_name: { type: 'UTF8' },
              }
            },
            version: { type: 'UTF8' }
          }
        },
        severity: { type: 'UTF8' },
        severity_id: { type: 'INT32' },
        status: { type: 'UTF8' },
        status_detail: { type: 'UTF8' },
        status_id: { type: 'INT32' },
        time: { type: 'INT64' },
        timezone_offset: { type: 'INT32' },
        type_name: { type: 'UTF8' },
        type_uid: { type: 'INT32' },
        user: {
          repeated: false,
          fields: {
            name: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            org_uid: { type: 'UTF8' },
          }
        },
      });      

      /*
        User Deleted Event
      */
    } else if (pingOneEvent?.action?.type === 'USER.DELETED') {

      let statusId = 0;

      if (pingOneEvent.result.status === 'SUCCESS') {
        statusId = 1;
      } else if (pingOneEvent.result.status === 'FAILED') {
        statusId = 2;
      }

      const isoStr = pingOneEvent.recordedAt;
      const date = new Date(isoStr);
      const timestamp = date.getTime();
      const timezone_offset = date.getTimezoneOffset();

      event_type_uid = 300106;

      ocsfEvent = {
        "activity_id": 6,
        "activity_name": "Delete",
        "category_name": "Audit Activity",
        "category_uid": 3,
        "class_name": "Account Change",
        "class_uid": 3001,
        "actor": {
          "idp": {
            "name": "PingOne",
            "uid": pingOneEvent.resources[0].environment.id
          },
          "user":  {
            "name": pingOneEvent.resources[0].name, // resources[0].name
            "uid": pingOneEvent.resources[0].id, // resources[0].id
          } 
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
        "type_name": "Account Change: De",
        "type_uid": event_type_uid,
        "user": {
          "name": pingOneEvent.resources[0].name, // resources[0].name
          "uid": pingOneEvent.resources[0].id, // resources[0].id
          "org_uid": pingOneEvent.resources[0].environment.id // resources[0].environment.id
        }
      }

      schemaEvent = new parquet.ParquetSchema({
        activity_id: { type: 'INT32' },
        category_name: { type: 'UTF8' },
        category_uid: { type: 'INT32' },
        class_name: { type: 'UTF8' },
        class_uid: { type: 'INT32' },
        actor: {
          repeated: false,
          fields: {
            idp: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }, 
            user: {
              repeated: false,
              fields: {
                name: { type: 'UTF8' },
                uid: { type: 'UTF8' },
              }
            }
          }
        },
        message: { type: 'UTF8' },
        metadata: {
          repeated: false,
          fields: {
            correlation_uid: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            original_time: { type: 'UTF8' },
            product: {
              repeated: false,
              fields: {
                lang: { type: 'UTF8' },
                name: { type: 'UTF8' },
                vendor_name: { type: 'UTF8' },
              }
            },
            version: { type: 'UTF8' }
          }
        },
        severity: { type: 'UTF8' },
        severity_id: { type: 'INT32' },
        status: { type: 'UTF8' },
        status_detail: { type: 'UTF8' },
        status_id: { type: 'INT32' },
        time: { type: 'INT64' },
        timezone_offset: { type: 'INT32' },
        type_name: { type: 'UTF8' },
        type_uid: { type: 'INT32' },
        user: {
          repeated: false,
          fields: {
            name: { type: 'UTF8' },
            uid: { type: 'UTF8' },
            org_uid: { type: 'UTF8' },
          }
        },
      });      



    } else {
      console.log(`OCSF event conversion for event type "${pingOneEvent?.action?.type}" not implemented, skipping event.`)

      // Skip events which are not mapped
      continue;
    }

    console.log(`Writing OCSF event to S3: ${ocsfEvent}`);

    // Write OCSF JSON objects to s3
    var ocsfKey = `${event_type_uid}_${roundedDate.toJSON()}`;
    const ocsfFullKey = `${process.env.SOURCE_LOCATION}/region=${process.env.REGION}/accountId=${process.env.ACCOUNT_ID}/eventHour=${eventHour}/${ocsfKey}.json`;

    const getParams = {
      Bucket: process.env.S3_BUCKET_OCSF,
      Key: ocsfFullKey
    }

    let objectExists = false;
    let prevData = [];

    try {
      await s3.headObject(getParams).promise()
      objectExists = true;
    } catch (err) {
      objectExists = false;
    }

    if (objectExists) {
      const prevDataResponse = await s3.getObject(getParams).promise();
      prevData = JSON.parse(prevDataResponse.Body.toString('utf-8'));
    }

    const mergedData = prevData.concat([ocsfEvent]);

    const uploadOCSFParams = {
      Bucket: process.env.S3_BUCKET_OCSF,
      Key: ocsfFullKey,
      Body: JSON.stringify(mergedData),
    };

    await s3.upload(uploadOCSFParams).promise();

    // End of ocsf event writing to S3

    // Write Parquet file from merged OCSF events    
    console.log(`Writing merged OCSF events to Parquet on S3: ${mergedData}`);

    var writer = await parquet.ParquetWriter.openFile(schemaEvent, '/tmp/auth.parquet');

    mergedData.forEach(async evt => {
      await writer.appendRow(evt);
    })

    await writer.close();

    const fileContents = fs.readFileSync('/tmp/auth.parquet');

    // assumeRole to destination S3 bucket
    aws.config.update({region: process.env.REGION});

    // Role data
    var roleToAssume = {RoleArn: process.env.EXTERNAL_ROLE_ARN,
                        ExternalId: process.env.EXTERNAL_ID,
                        RoleSessionName: 'PingOne_SecurityLake_Session',
                        DurationSeconds: 900};

    console.log("Role to Assume: ", roleToAssume);

    // Create the STS service object    
    var sts = new aws.STS({apiVersion: '2011-06-15'});

    const assumeRole = await sts.assumeRole(roleToAssume).promise();
    console.log('Role assumed successfully');

    const s3params = {
      apiVersion: '2006-03-01',
      accessKeyId: assumeRole.Credentials.AccessKeyId,
      secretAccessKey: assumeRole.Credentials.SecretAccessKey,
      sessionToken: assumeRole.Credentials.SessionToken,
    };

    const s3_assumed_role = new aws.S3(s3params);

    const parquetKey = `${event_type_uid}_${roundedDate.toJSON()}`;
    const parquetFullKey = `ext/${process.env.SOURCE_LOCATION}/region=${process.env.REGION}/accountId=${process.env.ACCOUNT_ID}/eventHour=${eventHour}/${parquetKey}.parquet`;

    const uploadParams = {
      Bucket: process.env.EXTERNAL_S3_BUCKET_PARQUET,
      Key: parquetFullKey,
      Body: fileContents,
    };

    await s3_assumed_role.upload(uploadParams).promise();

    console.log(`File ${parquetFullKey} successfully written to S3 bucket ${process.env.EXTERNAL_S3_BUCKET_PARQUET}`);
  }
}

function getEventHour(date) {
  var mm = date.getUTCMonth() + 1; // getMonth() is zero-based
  var dd = date.getUTCDate();
  var hh = date.getUTCHours();

  return [date.getFullYear(),
  (mm > 9 ? '' : '0') + mm,
  (dd > 9 ? '' : '0') + dd,
  (hh > 9 ? '' : '0') + hh
  ].join('');
}

exports.handler = async (event) => {

  const base64Credentials =  event.headers.authorization.split(' ')[1];
  const credentials = Buffer.from(base64Credentials, 'base64').toString('ascii');
  const [username, password] = credentials.split(':');

  if(username !== process.env.WEBHOOK_USERNAME || password !== process.env.WEBHOOK_PASSWORD) {
    console.error('Invalid credentials')

    return {
      statusCode: 403,
      body: {
        err_msg: "Invalid credentials"
      }
    }
  }


  // For testing purposes
  if (event.body) {
    pingOneEvent = JSON.parse(event.body);
  } else {
    pingOneEvent = event;
  }

  try {
    await convertToOCSF(pingOneEvent);

    // Success
    return { statusCode: 200 };

  } catch (err) {
    console.error(err);

    const error = {
      err_msg: err
    }

    return {
      statusCode: 500,
      body: JSON.stringify(error)
    };
  }
};

