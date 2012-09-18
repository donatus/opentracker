//
//  ot_filereplication.h
//  opentracker
//
//  Created by Stéphane Donnet on 13.09.12.
//
//

#ifndef opentracker_ot_filereplication_h
#define opentracker_ot_filereplication_h

#ifdef WANT_FILE_REPLICATION

void filereplication_init( );
void filereplication_deinit( );
void filereplication_deliver( int64 sock, ot_tasktype tasktype );

#else

#define filereplication_init()
#define filereplication_deinit()

#endif

#endif

