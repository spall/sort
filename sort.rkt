#lang racket

;; adapted from
;; racket/racket/collects/racket/private/sort.rkt

(#%require (rename '#%unsafe i+ unsafe-fx+)
           (rename '#%unsafe i- unsafe-fx-)
           (rename '#%unsafe i= unsafe-fx=)
           (rename '#%unsafe i< unsafe-fx<)
           (rename '#%unsafe i<= unsafe-fx<=)
           (rename '#%unsafe i>= unsafe-fx>=)
           (rename '#%unsafe i>> unsafe-fxrshift)
           (rename '#%unsafe vref unsafe-vector-ref)
           (rename '#%unsafe vset! unsafe-vector-set!)
           future-visualizer)

(define-syntax-rule (<? x y)
  (i< x y))


(define (custom-4xparallel-sort lst)
  (define n (length lst))

  (define (merge v1 n1 v2 n2 new-vec)
    (let loop ([i 0] [pos1 0] [pos2 0])
      (cond
        [(i>= i (vector-length new-vec))
         new-vec]
        [(i>= pos1 n1)
         (begin (for-each (lambda (i_ pos_) (vset! new-vec i_ (vref v2 pos_)))
                          (range i (i+ n1 n2)) (range pos2 n2))
                new-vec)]
         ;;(begin (vset! new-vec i (vref v2 pos2))
           ;;     (loop (i+ 1 i) pos1 (i+ 1 pos2)))] 
        [(i>= pos2 n2)
         (begin (for-each (lambda (i_ pos_) (vset! new-vec i_ (vref v1 pos_)))
                          (range i (i+ n1 n2)) (range pos1 n1))
                new-vec)]
       ;;  (begin (vset! new-vec i (vref v1 pos1))
         ;;       (loop (i+ 1 i) (i+ 1 pos1) pos2))]
        [else
         (let ([ref1 (vref v1 pos1)]
               [ref2 (vref v2 pos2)])
           (if (<? ref1 ref2)
                (begin (vset! new-vec i ref1)
                       (loop (i+ 1 i) (i+ 1 pos1) pos2))
                (begin (vset! new-vec i ref2)
                       (loop (i+ 1 i) pos1 (i+ 1 pos2)))))])))
        
  (let* ([q1 (i>> (i>> n 1) 1)]
         [q2 (i- (i>> n 1) q1)]
         [q3 (i>> (i- n (i>> n 1)) 1)]
         [q4 (i- (i- n (i>> n 1)) q3)]
         [vec1 (make-vector (i+ q1 (ceiling (/ q1 2))))]
         [vec2 (make-vector (i+ q2 (ceiling (/ q2 2))))]
         [vec3 (make-vector (i+ q3 (ceiling (/ q3 2))))]
         [vec4 (make-vector (i+ q4 (ceiling (/ q4 2))))])
    ;; list -> vector
    (define (loop i stop v ls)
      (cond
        [(empty? ls)
         ls]
        [(i< i stop)
         (begin 
           (vset! v i (car ls))
           (loop (i+ i 1) stop v (cdr ls)))]
        [else
         ls]))
    
    (loop 0 q4 vec4 (loop 0 q3 vec3 (loop 0 q2 vec2 (loop 0 q1 vec1 lst))))

    (sort (make-vector 15 2) 10)
    
    (let ([f1 (future (lambda () 
                         (sort vec1 q1)))]
          [f2 (future (lambda ()  (sort vec2 q2)))]
          [f3 (future (lambda ()  (sort vec3 q3)))])
         ;; [f4 (future (lambda () (sort vec4 q4)))])
      (sort vec4 q4);; (time (sort vec4 q4))
      (touch f1) (touch f2) (touch f3)
      ;; merge the four vectors
      ;; vector -> list
      ;; this is murder. redo it.
      (let* ([nv1 (make-vector (i+ q1 q2))]
             [nv2 (make-vector (i+ q3 q4))]
             [fm1 (future (lambda () (merge vec1 q1 vec2 q2 nv1)))]
             [mres (merge vec3 q3 vec4 q4 nv2)]
             [nv3 (make-vector (i+ (i+ q1 q2) (i+ q3 q4)))])
        (merge mres (i+ q3 q4) (touch fm1) (i+ q1 q2) nv3)))))


;; take a vector instead of list
(define (custom-parallel-sort lst)
  (define n (length lst))
  (define half1 (i>> n 1))
  (define half2 (i- n half1))
  (let ([vec1 (make-vector (i+ half1 (ceiling (/ half1 2))))]
        [vec2 (make-vector (i+ half2 (ceiling (/ half2 2))))])
    ;; list -> vector
    ;; half1
    (let loop ([i 0] [lst lst])
      (cond
        [(empty? lst)
         void]
        [(i< i half1)
         (begin (vset! vec1 i (car lst))
                (loop (i+ 1 i) (cdr lst)))]
        [else
         (begin (vset! vec2 (i- i half1) (car lst))
                (loop (i+ 1 i) (cdr lst)))]))

    (sort (make-vector 15 2) 10)
    
    (define f1 (future (lambda ()
                         (sort vec1 half1)))) ;;
    (sort vec2 half2) ;; time each half
    (touch f1)
    ;; merge the two vectors
    ;; vector -> list
    ;; time merge
    (let loop ([i n] [pos1 (- half1 1)] [pos2 (- half2 1)] [r '()])
      (let ([i (i- i 1)])
        (if (i< i 0)
            r
            (cond
              [(i< pos1 0)
               (loop i pos1 (i- pos2 1) (cons (vref vec2 pos2) r))]
              [(i< pos2 0)
               (loop i (i- pos1 1) pos2 (cons (vref vec1 pos1) r))]
              [(<? (vref vec1 pos1) (vref vec2 pos2))
               (loop i pos1 (i- pos2 1) (cons (vref vec2 pos2) r))]
              [else
               (loop i (i- pos1 1) pos2 (cons (vref vec1 pos1) r))]))))))

;; spawn 2 more futures.
;; can parallelize merge?
;; github.iu
;; create one that uses just vectors, doesnt go from list to vector.

(define (custom-sort lst)
  (define n (length lst))
  (let ([vec (make-vector (i+ n (ceiling (/ n 2))))])
    ;; list -> vector
    (let loop ([i 0] [lst lst])
      (when (pair? lst)
        (vset! vec i (car lst))
        (loop (i+ 1 i) (cdr lst))))
    (sort vec n)
    ;; vector -> list
    (let loop ([i n] [r '()])
      (let ([i (i- i 1)])
        (if (i< i 0) r (loop i (cons (vref vec i) r)))))))

(define (sort v n)
  (let* ([n/2- (i>> n 1)] [n/2+ (i- n n/2-)])
    
    (define-syntax-rule (ref n) (vref v n))
    (define-syntax-rule (set! n x) (vset! v n x))

    (define-syntax-rule (merge lo? A1 A2 B1 B2 C1)
      (let ([b2 B2])
        (let loop ([a1 A1] [b1 B1] [c1 C1])
          (let ([x (ref a1)] [y (ref b1)])
            (if (if lo? (not (<? y x)) (<? x y))
              (begin (set! c1 x)
                     (let ([a1 (i+ a1 1)] [c1 (i+ c1 1)])
                       (when (i< c1 b1) (loop a1 b1 c1))))
              (begin (set! c1 y)
                     (let ([b1 (i+ b1 1)] [c1 (i+ c1 1)])
                       (if (i<= b2 b1)
                         (let loop ([a1 a1] [c1 c1])
                           (when (i< c1 b1)
                             (set! c1 (ref a1))
                             (loop (i+ a1 1) (i+ c1 1))))
                         (loop a1 b1 c1)))))))))

    (define-syntax-rule (copying-insertionsort Alo Blo n)
      ;; n is never 0
      (begin (set! Blo (ref Alo))
             (let iloop ([i 1])
               (when (i< i n)
                 (let ([ref-i (ref (i+ Alo i))])
                   (let jloop ([j (i+ Blo i)])
                     (let ([ref-j-1 (ref (i- j 1))])
                       (if (and (i< Blo j) (<? ref-i ref-j-1))
                         (begin (set! j ref-j-1) (jloop (i- j 1)))
                         (begin (set! j ref-i) (iloop (i+ i 1)))))))))))

    (define (copying-mergesort Alo Blo n)
      (cond
        ;; n is never 0, smaller values are more frequent
        [(i= n 1) (set! Blo (ref Alo))]
        [(i= n 2) (let ([x (ref Alo)] [y (ref (i+ Alo 1))])
                    (if (<? y x)
                      (begin (set! Blo y) (set! (i+ Blo 1) x))
                      (begin (set! Blo x) (set! (i+ Blo 1) y))))]
        ;; insertion sort for small chunks (not much difference up to ~30)
        [(i< n 16) (copying-insertionsort Alo Blo n)] 
        [else (let* ([n/2- (i>> n 1)] [n/2+ (i- n n/2-)])
                (let ([Amid1 (i+ Alo n/2-)]
                      [Amid2 (i+ Alo n/2+)]
                      [Bmid1 (i+ Blo n/2-)])
                  (copying-mergesort Amid1 Bmid1 n/2+)
                  (copying-mergesort Alo Amid2 n/2-)
                  (merge #t Amid2 (i+ Alo n) Bmid1 (i+ Blo n) Blo)))]))
    
    (let ([Alo 0] [Amid1 n/2-] [Amid2 n/2+] [Ahi n] [B1lo n])
      (copying-mergesort Amid1 B1lo n/2+)
      (unless (zero? n/2-)
        (copying-mergesort Alo Amid2 n/2-))
       (merge #f B1lo (i+ B1lo n/2+) Amid2 Ahi Alo))))


;;(custom-sort (shuffle (range 1000)))
;;(custom-parallel-sort (shuffle (range 1000)))
;;(custom-4xparallel-sort (shuffle (range 12)))

;;(visualize-futures (custom-4xparallel-sort (shuffle (range 100000))))
;; add more tests

;; add timing code


(define LOOPNUM 100)
(define SIZE 100000)
(define ls (for/list ([_ (in-range SIZE)]) (random 1000000)))

(displayln "timing non parallel sort")
(time (for ([_ (in-range LOOPNUM)])
        (custom-sort ls)))

(collect-garbage)
(collect-garbage)
(collect-garbage)

(displayln "timing 4 thread sort")
(time (for ([_ (in-range LOOPNUM)])
        (custom-4xparallel-sort ls)))

(collect-garbage)
(collect-garbage)
(collect-garbage)

(displayln "timing 2 thread sort")
(time (for ([_ (in-range LOOPNUM)])
        (custom-parallel-sort ls)))










