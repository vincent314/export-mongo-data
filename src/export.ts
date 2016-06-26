import mongodb = require('mongodb');
import chalk = require('chalk');
import {MongoError} from "mongodb";
import * as _ from 'lodash';
import {Db} from "mongodb";
import Track = vince.Track;
import Event = vince.Event;
import FB = vince.FB;
import fs = require('fs');
import Kml = vince.Kml;
import firebase = require('firebase');
const path = require('path');
import ThenableReference = firebase.database.ThenableReference;
import moment = require('moment');
import {secret} from './secret';

const MongoClient = mongodb.MongoClient;
const url = secret.mongoUrl;

let app:firebase.app.App = firebase.initializeApp({
    databaseURL: "https://fire-rol.firebaseio.com",
    serviceAccount: path.join(__dirname, "fire-rol-5a1ea7e7155a.json")
});

let mongoDb;

cleanFirebase().then(()=> {
    return MongoClient.connect(url);
}).then((db:Db)=> {
    mongoDb = db;
    return Promise.all([findTracks(db), findEvents(db)])
}).then((results):any=> {
    // const [tracks, events] = results;
    const tracks = results[0];
    const events = results[1];

    mongoDb.close();

    return {
        tracks: _.map(tracks, mapTrack),
        events: _.map(events, mapEvent),
        kmls: _.map(tracks, (t:Track) => t.kml)
    };
}).then(result => {
    console.log(`Saving ${result.tracks.length} values`);

    const tracks:Track[] = _.map(result.tracks, (t:Track) => {
        const keyKml = push('kmls', t.kml).key;
        t.kml = keyKml;
        const keyTrack = push('tracks', _.omit(t, ['idMongo', 'idFirebase'])).key;
        t.idFirebase = keyTrack;
        return t;
    });


    const tracksByIdMongo = _.keyBy(tracks, t => t.idMongo);

    const mongo2firebase = (id:string):string => {
        if (!id) return null;
        const track:Track = tracksByIdMongo[id];
        if (!track) return null;
        return track.idFirebase;
    };

    // convert loop ids from mongo to firebase
    _.each(result.events, e => {
        e.loop1 = mongo2firebase(e.loop1);
        e.loop2 = mongo2firebase(e.loop2);
        e.loop3 = mongo2firebase(e.loop3);
        push('events', e);
    });
}).catch((err:MongoError|firebase.FirebaseError)=> {
    console.log(chalk.red(err.message));
});

function cleanFirebase() {
    return app.database().ref().remove();
}

function push(path, value):ThenableReference {
    const pushedValue = app.database().ref(path).push(value, (err)=> {
        if (err) {
            console.log(err)
        } else {
            console.log(`Value pushed with key : ${pushedValue.key}`);
        }
    });
    return pushedValue;

}

function findTracks(db:Db):Promise<any> {
    return db.collection('tracks').find({})
        .toArray()
        .then((data:any)=> {
            return data;
        });
}

function findEvents(db:Db):Promise<any> {
    return db.collection('events').find({})
        .toArray()
        .then((data:any)=> {
            return data;
        });
}

function mapTrack(t:any):Track {
    const track:Track = _.pick<Track,any>(t, ['name', 'distance', 'type', 'kml']);
    track.idMongo = t._id.toString();
    return track;
}

function mapEvent(e:any):Event {
    const event:Event = _.pick<Event,any>(e, ['name', 'type', 'loop1', 'loop2', 'loop3', 'dateTime']);
    event.dateTime = e.dateTime.getTime();
    return event;
}