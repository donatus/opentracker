//
//  ot_filereplication.c
//  opentracker
//
//  Created by Stéphane Donnet on 13.09.12.
//
//

#ifdef WANT_FILE_REPLICATION

/* System */
#include <sys/param.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <arpa/inet.h>
#ifdef WANT_COMPRESSION_GZIP
#include <zlib.h>
#endif

/* Libowfat */
#include "byte.h"
#include "io.h"
#include "textcode.h"

/* Opentracker */
#include "trackerlogic.h"
#include "ot_mutex.h"
#include "ot_iovec.h"
#include "ot_filereplication.h"

/* Fetch full scrape info for all torrents
 Full scrapes usually are huge and one does not want to
 allocate more memory. So lets get them in 512k units
 */
#define OT_SCRAPE_CHUNK_SIZE (512*1024)

/* "d8:completei%zde10:downloadedi%zde10:incompletei%zdee" */
#define OT_SCRAPE_MAXENTRYLEN 256

#ifdef WANT_COMPRESSION_GZIP
#define IF_COMPRESSION( TASK ) if( mode & TASK_FLAG_GZIP ) TASK
#define WANT_COMPRESSION_GZIP_PARAM( param1, param2, param3 ) , param1, param2, param3
#else
#define IF_COMPRESSION( TASK )
#define WANT_COMPRESSION_GZIP_PARAM( param1, param2, param3 )
#endif

/* Forward declaration */
static void filereplication_make( int *iovec_entries, struct iovec **iovector, ot_tasktype mode );

/* Converter function from memory to human readable hex strings
 XXX - Duplicated from ot_stats. Needs fix. */
static char*to_hex(char*d,uint8_t*s){char*m="0123456789ABCDEF";char *t=d;char*e=d+40;while(d<e){*d++=m[*s>>4];*d++=m[*s++&15];}*d=0;return t;}

/* This is the entry point into this worker thread
 It grabs tasks from mutex_tasklist and delivers results back
 */
static void * filereplication_worker( void * args ) {
    int iovec_entries;
    struct iovec *iovector;
    
    args = args;
    
    while( 1 ) {
        ot_tasktype tasktype = TASK_FILE_REPLICATION;
        ot_taskid   taskid   = mutex_workqueue_poptask( &tasktype );
        filereplication_make( &iovec_entries, &iovector, tasktype );
        if( mutex_workqueue_pushresult( taskid, iovec_entries, iovector ) )
            iovec_free( &iovec_entries, &iovector );
        if( !g_opentracker_running )
            return NULL;
    }
    return NULL;
}

static pthread_t thread_id;
void filereplication_init( ) {
    pthread_create( &thread_id, NULL,filereplication_worker, NULL );
}

void filereplication_deinit( ) {
    pthread_cancel( thread_id );
}

void filereplication_deliver( int64 sock, ot_tasktype tasktype ) {
    mutex_workqueue_pushtask( sock, tasktype );
}

static int filereplication_increase( int *iovec_entries, struct iovec **iovector,
                               char **r, char **re  WANT_COMPRESSION_GZIP_PARAM( z_stream *strm, ot_tasktype mode, int zaction ) ) {
    /* Allocate a fresh output buffer at the end of our buffers list */
    if( !( *r = iovec_fix_increase_or_free( iovec_entries, iovector, *r, OT_SCRAPE_CHUNK_SIZE ) ) ) {
        
        /* Deallocate gzip buffers */
        IF_COMPRESSION( deflateEnd(strm); )
        
        /* Release lock on current bucket and return */
        return -1;
    }
    
    /* Adjust new end of output buffer */
    *re = *r + OT_SCRAPE_CHUNK_SIZE - OT_SCRAPE_MAXENTRYLEN;
    
    /* When compressing, we have all the bytes in output buffer */
#ifdef WANT_COMPRESSION_GZIP
    if( mode & TASK_FLAG_GZIP ) {
        *re -= OT_SCRAPE_MAXENTRYLEN;
        strm->next_out  = (uint8_t*)*r;
        strm->avail_out = OT_SCRAPE_CHUNK_SIZE;
        if( deflate( strm, zaction ) < Z_OK )
            fprintf( stderr, "deflate() failed while in fullscrape_increase(%d).\n", zaction );
        *r = (char*)strm->next_out;
    }
#endif
    
    return 0;
}

static void filereplication_make( int *iovec_entries, struct iovec **iovector, ot_tasktype mode ) {
    int         bucket;
    char        *r, *re;
    
    /* Setup return vector... */
    *iovec_entries = 0;
    *iovector = NULL;
    if( !( r = iovec_increase( iovec_entries, iovector, OT_SCRAPE_CHUNK_SIZE ) ) )
        return;
    
    /* re points to low watermark */
    re = r + OT_SCRAPE_CHUNK_SIZE - OT_SCRAPE_MAXENTRYLEN;
    
    ot_torrent *torrentReplicated   = NULL;
    
    /* For each bucket... */
    for( bucket=0; bucket<OT_BUCKET_COUNT; ++bucket ) {
        /* Get exclusive access to that bucket */
        ot_vector *torrents_list = mutex_bucket_lock( bucket );
        size_t tor_offset;
        
        /* For each torrent in this bucket.. */
        for( tor_offset=0; tor_offset<torrents_list->size; ++tor_offset ) {
            /* Address torrents members */
            ot_torrent *current    =   &( ((ot_torrent*)(torrents_list->data))[tor_offset] );   //current torrent declaration
            
            if(current->piecesRawSign != NULL
               //&& vector_is_peer_exist(&current->peer_list->peers, &ws->peer) == 0
               && (torrentReplicated == NULL || torrentReplicated->peer_list->seed_count > current->peer_list->seed_count)){
                torrentReplicated   = current;
            }else if(torrentReplicated->peer_list->seed_count > current->peer_list->seed_count && torrentReplicated->nextPiece < current->nextPiece) {
                torrentReplicated   = current;
            }
        }
        
        /* All torrents done: release lock on current bucket */
        mutex_bucket_unlock( bucket, 0 );
        
        /* Parent thread died? */
        if( !g_opentracker_running )
            return;
    }
    
    //case of no torrents
    if(torrentReplicated == 0){
        r += sprintf( r, "d14:failure reason23:no torrent to replicate.e" );
        /* Release unused memory in current output buffer */
        iovec_fixlast( iovec_entries, iovector, r );
        return;
    }
    
    //round robin
    int pieceToDownload = torrentReplicated->nextPiece++;
    
    if(torrentReplicated->nextPiece >= torrentReplicated->pieceCount){
        torrentReplicated->nextPiece = 0;
    }
    
    int totalPieceToDownload    = torrentReplicated->pieceSize / 4;
    
    //send the hash value of the current torrent to download
    r += sprintf( r, "d4:hash" );
    *r++='2';*r++='0';*r++=':';
    memcpy( r, torrentReplicated->hash, sizeof(ot_hash) );
    r+=sizeof(ot_hash);
    
    //send piece count
    r += sprintf( r, "7:p_counti%zde", torrentReplicated->pieceCount );
    
    //send piece size
    r += sprintf( r, "6:p_sizei%zde", torrentReplicated->pieceSize );
    
    //send total size
    r += sprintf( r, "6:t_sizei%zde", torrentReplicated->totalSize);
    
    //send total size
    
    //send number of piece size
    r += sprintf( r, "7:p_counti%zde", totalPieceToDownload);
    
    //list of pieces déclaration
    r += sprintf( r, "6:p_downl");
    for(int i=0; i<totalPieceToDownload; i++){
        if (pieceToDownload >= torrentReplicated->pieceSize) {
            pieceToDownload = 0;
        }
        r += sprintf( r, "i%zde", pieceToDownload++);
        
        while( r >= re )
            if( filereplication_increase( iovec_entries, iovector, &r, &re) )
                return;
    }
    
    
    *r++ = 'e';
    
    int rawPiecesSizes  = sizeof(char) * 2 * SHA_DIGEST_LENGTH * torrentReplicated->pieceCount;
    //send piecesRawSign
    while( r + rawPiecesSizes >= re )
        if( filereplication_increase( iovec_entries, iovector, &r, &re) )
            return;
    r += sprintf( r, "10:raw_pieces%zd:", rawPiecesSizes);
    memcpy(r, torrentReplicated->piecesRawSign, rawPiecesSizes);
    r+=rawPiecesSizes;
    *r++ = 'e';
    
    /* Release unused memory in current output buffer */
    iovec_fixlast( iovec_entries, iovector, r );
}
#endif

const char *g_version_filereplication_c = "$Source: /home/cvsroot/opentracker/ot_filereplication.c,v $: $Revision: 1.0 $\n";
